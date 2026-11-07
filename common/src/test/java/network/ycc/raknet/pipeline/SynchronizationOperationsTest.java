package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import network.ycc.raknet.config.DefaultConfig;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.UINT;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks typed reset operations against actual scheduler ownership and wrapped ordering cursors. */
class SynchronizationOperationsTest {
    /** Detached unsent and outstanding frames remain owned exactly once and restore byte accounting and promises. */
    @Test
    void detachingAndRestoringFramesBalancesOwnership() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final ReliabilityHandler handler = new ReliabilityHandler();
        handler.config = new DefaultConfig(channel);
        final Frame queued = frame(channel);
        final Frame sent = frame(channel);
        final FrameSet set = FrameSet.create();
        queued.setPromise(channel.newPromise());
        sent.setPromise(channel.newPromise());
        handler.queueFrame(queued);
        set.addPacket(sent);
        handler.pendingFrameSets.put(3, set);
        final List<Frame> detached = handler.takePendingFrames();
        try {
            assertEquals(2, detached.size());
            assertTrue(handler.frameQueue.isEmpty());
            assertTrue(handler.pendingFrameSets.isEmpty());
            assertEquals(0, handler.queuedBytes);
            assertEquals(0, set.refCnt());
            assertEquals(1, queued.refCnt());
            assertEquals(1, sent.refCnt());
            assertFalse(queued.getPromise().isDone());
            detached.forEach(handler::restorePendingFrame);
            detached.clear();
            assertEquals(queued.getRoughPacketSize() + sent.getRoughPacketSize(), handler.queuedBytes);
        } finally {
            detached.forEach(Frame::release);
            handler.clearQueue(new IllegalStateException("Fixture finished"));
            channel.finishAndReleaseAll();
        }
        assertEquals(0, queued.refCnt());
        assertEquals(0, sent.refCnt());
    }

    /** A reset releases skipped payloads while keeping later positions and refusing stale cursor rewind. */
    @Test
    void orderingResetPreservesFutureFrames() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final FrameOrderIn ordering = new FrameOrderIn();
        final Frame old = frame(channel);
        final Frame future = frame(channel);
        final FrameOrderIn.OrderedChannelPacketQueue lane = ordering.channels[0];
        final FrameData oldData = old.retainedFrameData();
        lane.enqueue(4, oldData);
        lane.enqueue(9, future.retainedFrameData());
        try {
            ordering.skipOrderedThrough(0, 5);
            assertEquals(5, lane.lastOrderIndex);
            assertFalse(lane.queue.containsKey(4));
            assertTrue(lane.queue.containsKey(9));
            assertEquals(1, oldData.refCnt());
            ordering.skipOrderedThrough(0, 3);
            assertEquals(5, lane.lastOrderIndex);
            assertThrows(IllegalArgumentException.class, () -> ordering.skipOrderedThrough(0, 0x1000000));
            assertEquals(5, lane.lastOrderIndex);
        } finally {
            lane.clear();
            old.release();
            future.release();
            channel.finishAndReleaseAll();
        }
    }

    /** Gap tracking advances across wrap but a delayed synchronization marker cannot rewind it. */
    @Test
    void receiveCursorOnlyAdvances() {
        final ReliabilityHandler handler = new ReliabilityHandler();
        handler.lastReceivedSeqId = UINT.B3.MAX_VALUE - 1;
        handler.nackSet.add(UINT.B3.MAX_VALUE);
        handler.nackSet.add(1);
        handler.skipReceivedThrough(0);
        assertEquals(0, handler.lastReceivedSeqId);
        assertFalse(handler.nackSet.contains(UINT.B3.MAX_VALUE));
        assertTrue(handler.nackSet.contains(1));
        handler.skipReceivedThrough(UINT.B3.MAX_VALUE);
        assertEquals(0, handler.lastReceivedSeqId);
        assertThrows(IllegalArgumentException.class, () -> handler.skipReceivedThrough(-1));
        handler.lastReceivedSeqId = 5;
        handler.skipReceivedThrough(0);
        assertTrue(handler.nackSet.contains(1));
    }

    /**
     * Creates a separately owned ordered payload for reference-count and scheduler assertions.
     *
     * @param channel fixture allocator and promise owner
     * @return exactly one owned frame reference
     */
    private static Frame frame(final EmbeddedChannel channel) {
        final ByteBuf bytes = Unpooled.buffer().writeByte(42);
        final FrameData data = FrameData.create(channel.alloc(), 0xfd, bytes);
        try {
            data.setReliability(FramedPacket.Reliability.RELIABLE_ORDERED);
            data.setOrderChannel(0);
            return Frame.createOrdered(data, 0, 0);
        } finally {
            data.release();
            bytes.release();
        }
    }
}
