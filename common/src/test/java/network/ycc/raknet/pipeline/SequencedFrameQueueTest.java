package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket.Reliability;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises sequenced gap advancement with owned queued frames and wrapping wire counters. */
class SequencedFrameQueueTest {
    /** Skipping a gap releases every superseded payload, including the position just before the new frame. */
    @Test
    void releasesImmediatelyPrecedingQueuedFrame() {
        verifyGap(-1, 2, 3, 5);
    }

    /** The same ownership rule holds when order counters cross the twenty-four-bit wrap boundary. */
    @Test
    void releasesSkippedFrameAcrossCounterWrap() {
        verifyGap(0xfffffd, 0xffffff, 0, 2);
    }

    /** A sparse queue remains correct when a peer jumps millions of sequence positions. */
    @Test
    void handlesLargeSparseGap() {
        verifyGap(-1, 0x6fffff, 0x700000, 0x700002);
    }

    /**
     * Checks release of a skipped frame while preserving a later queued frame and the delivered payload.
     *
     * @param previous last delivered order position, including the initial minus-one sentinel
     * @param skipped queued position superseded by the incoming sequenced frame
     * @param arriving new sequenced order position
     * @param future queued position that must remain pending after advancement
     */
    private static void verifyGap(final int previous, final int skipped,
            final int arriving, final int future) {
        final FrameOrderIn.OrderedChannelPacketQueue queue = new FrameOrderIn.OrderedChannelPacketQueue();
        queue.lastOrderIndex = previous;
        final List<Object> delivered = new ArrayList<>();
        final FrameData obsolete = payload();
        final FrameData pending = payload();
        queue.queue.put(skipped, obsolete);
        queue.queue.put(future, pending);
        final Frame frame = sequencedFrame(arriving);
        try {
            queue.decodeSequenced(frame, delivered);
            assertEquals(arriving, queue.lastOrderIndex);
            assertEquals(1, delivered.size());
            assertEquals(0, obsolete.refCnt());
            assertFalse(queue.queue.containsKey(skipped));
            assertTrue(queue.queue.containsKey(future));
            assertEquals(1, pending.refCnt());
        } finally {
            frame.release();
            delivered.forEach(ReferenceCountUtil::release);
            queue.clear();
        }
        assertEquals(0, pending.refCnt());
    }

    /**
     * Creates a sequenced frame while balancing the payload reference retained by the frame factory.
     *
     * @param order position at which the frame supersedes earlier queued payloads
     * @return reliable sequenced frame with one caller-owned reference
     */
    private static Frame sequencedFrame(final int order) {
        final FrameData data = payload();
        try {
            data.setReliability(Reliability.RELIABLE_SEQUENCED);
            data.setOrderChannel(0);
            return Frame.createOrdered(data, order, 0);
        } finally {
            data.release();
        }
    }

    /**
     * Creates a small framed payload and releases the input reference retained by its factory.
     *
     * @return payload with exactly one caller-owned reference
     */
    private static FrameData payload() {
        final ByteBuf bytes = Unpooled.buffer(1).writeByte(42);
        try {
            return FrameData.create(UnpooledByteBufAllocator.DEFAULT, 0xfd, bytes);
        } finally {
            bytes.release();
        }
    }
}
