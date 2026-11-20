package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.FrameSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies rejection ownership and write completion before a caller flushes the transport. */
class ReliabilityWriteBudgetTest {
    /** A frame larger than the MTU fails its caller and releases its input even without a flush. */
    @Test
    void oversizedWriteReleasesItsReference() {
        final RakNetEmbeddedChannel channel = new RakNetEmbeddedChannel(new ReliabilityHandler());
        final Frame frame = frame(channel.config().getMTU());
        try {
            final ChannelFuture write = channel.write(frame);
            assertTrue(write.cause() instanceof CorruptedFrameException);
            assertEquals(0, frame.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /** Omitting flush cannot bypass the queue limit, and an accepted write retains its own completion. */
    @Test
    void queueBudgetIsEnforcedBeforeFlush() {
        final ReliabilityHandler handler = new ReliabilityHandler();
        final RakNetEmbeddedChannel channel = new RakNetEmbeddedChannel(handler);
        channel.config().setMaxQueuedBytes(52);
        final Frame first = frame(1);
        final Frame second = frame(1);
        final Frame rejected = frame(1);
        try {
            final ChannelFuture firstWrite = channel.write(first);
            final ChannelFuture secondWrite = channel.write(second);
            final ChannelFuture rejectedWrite = channel.write(rejected);
            assertTrue(rejectedWrite.cause() instanceof TooLongFrameException);
            assertEquals(0, rejected.refCnt());
            assertFalse(firstWrite.isDone());
            assertFalse(secondWrite.isDone());
            assertEquals(52, handler.queuedBytes);
            channel.pipeline().remove(handler);
            assertTrue(firstWrite.isDone());
            assertTrue(secondWrite.isDone());
            assertEquals(0, first.refCnt());
            assertEquals(0, second.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /** A rejected retransmission releases both frame-set and retry references and fails all affected promises. */
    @Test
    void rejectedRetryReleasesEveryOwner() {
        final ReliabilityHandler handler = new ReliabilityHandler();
        final RakNetEmbeddedChannel channel = new RakNetEmbeddedChannel(handler);
        channel.config().setMaxQueuedBytes(26);
        final Frame queued = frame(1);
        final Frame sent = frame(1);
        final ChannelPromise sentPromise = channel.newPromise();
        sent.setPromise(sentPromise);
        final FrameSet set = FrameSet.create();
        set.addPacket(sent);
        try {
            final ChannelFuture queuedWrite = channel.write(queued);
            final TooLongFrameException failure = assertThrows(TooLongFrameException.class,
                    () -> handler.recallFrameSet(set));
            assertSame(failure, sentPromise.cause());
            assertSame(failure, queuedWrite.cause());
            assertEquals(0, sent.refCnt());
            assertEquals(0, queued.refCnt());
            assertEquals(0, set.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /** Only foreground bytes change writability, while every frame consumes the hard budget. */
    @Test
    void backgroundDoesNotSetTheGameplayWatermark() {
        final ReliabilityHandler handler = new ReliabilityHandler();
        final RakNetEmbeddedChannel channel = new RakNetEmbeddedChannel(handler);
        channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(40, 80));
        channel.config().setMaxQueuedBytes(512);
        handler.setBackgroundOrderChannel(6);
        try {
            channel.write(frame(256, 6));
            handler.updateBackPressure(channel.pipeline().context(handler));
            assertTrue(channel.attr(RakNet.WRITABLE).get());
            assertEquals(281, handler.getQueuedBytes());

            channel.write(frame(60, 7));
            handler.updateBackPressure(channel.pipeline().context(handler));
            assertFalse(channel.attr(RakNet.WRITABLE).get());
            for (Frame pending : handler.takePendingFrames()) {
                if (pending.getOrderChannel() == 6) {
                    handler.restorePendingFrame(pending);
                } else {
                    pending.getPromise().setSuccess();
                    pending.release();
                }
            }
            handler.updateBackPressure(channel.pipeline().context(handler));
            assertTrue(channel.attr(RakNet.WRITABLE).get());
            assertEquals(281, handler.getQueuedBytes());

            final ChannelFuture rejected = channel.write(frame(256, 6));
            assertTrue(rejected.cause() instanceof TooLongFrameException);
            assertEquals(281, handler.getQueuedBytes());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Creates an ordered packet with a selected payload size and one caller-owned frame reference.
     *
     * @param size application bytes, excluding the packet identifier and frame header
     * @return frame to transfer into an outbound queue or outstanding frame set
     */
    private static Frame frame(final int size) {
        return frame(size, 0);
    }

    /**
     * Creates a FIFO packet on the selected ordering channel.
     *
     * @param size application bytes excluding the packet identifier
     * @param orderChannel transport channel
     * @return caller-owned frame
     */
    private static Frame frame(final int size, final int orderChannel) {
        final ByteBuf bytes = Unpooled.buffer(size).writeZero(size);
        final FrameData data = FrameData.create(bytes.alloc(), 0xfd, bytes);
        try {
            data.setOrderChannel(orderChannel);
            return Frame.createOrdered(data, 0, 0);
        } finally {
            data.release();
            bytes.release();
        }
    }
}
