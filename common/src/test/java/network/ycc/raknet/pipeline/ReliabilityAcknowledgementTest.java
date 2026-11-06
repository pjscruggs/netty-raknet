package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import network.ycc.raknet.config.DefaultConfig;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.utils.UINT;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises actual ACK completion and NACK recall with sparse outstanding frame sets. */
class ReliabilityAcknowledgementTest {
    /** Repeated full-space ACKs release each outstanding frame once without expanding the ranges. */
    @Test
    void broadAcknowledgementsReleasePendingFramesOnce() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final TestHandler handler = new TestHandler(channel);
        final FrameSet first = handler.pending(7);
        final FrameSet second = handler.pending(UINT.B3.MAX_VALUE);
        try {
            handler.readAck(decode(new Reliability.ACK(), 0, UINT.B3.MAX_VALUE, 1000));
            assertTrue(handler.pendingFrameSets.isEmpty());
            assertEquals(0, first.refCnt());
            assertEquals(0, second.refCnt());
            assertEquals(2, handler.productionRequests);
            handler.readAck(new Reliability.ACK(7));
            assertEquals(2, handler.productionRequests);
        } finally {
            handler.clearQueue(null);
            channel.finishAndReleaseAll();
        }
    }

    /** Wrapped NACKs recall only their covered frame sets and respect disabled NACK handling. */
    @Test
    void negativeAcknowledgementsRecallCoveredFramesOnce() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final TestHandler handler = new TestHandler(channel);
        final FrameSet first = handler.pending(UINT.B3.MAX_VALUE);
        final FrameSet second = handler.pending(0);
        final FrameSet outside = handler.pending(8);
        try {
            final Reliability.NACK nack = decode(new Reliability.NACK(), UINT.B3.MAX_VALUE, 2, 2);
            handler.readNack(nack);
            assertEquals(3, handler.pendingFrameSets.size());
            handler.config.setNACKEnabled(true);
            handler.readNack(nack);
            assertFalse(handler.pendingFrameSets.containsKey(0));
            assertFalse(handler.pendingFrameSets.containsKey(UINT.B3.MAX_VALUE));
            assertEquals(0, first.refCnt());
            assertEquals(0, second.refCnt());
            assertEquals(1, outside.refCnt());
            assertEquals(2, handler.productionRequests);
        } finally {
            handler.clearQueue(null);
            channel.finishAndReleaseAll();
        }
        assertEquals(0, outside.refCnt());
    }

    /**
     * Uses the real wire decoder to populate repeated inclusive acknowledgement ranges.
     *
     * @param <T> concrete acknowledgement type returned after decoding
     * @param packet empty ACK or NACK instance
     * @param start first sequence number
     * @param end last sequence number, possibly wrapping through zero
     * @param count number of repeated entries
     * @return decoded packet
     */
    private static <T extends Reliability> T decode(final T packet, final int start,
            final int end, final int count) {
        final ByteBuf bytes = Unpooled.buffer().writeShort(count);
        try {
            for (int index = 0; index < count; index++) {
                bytes.writeBoolean(false).writeMediumLE(start).writeMediumLE(end);
            }
            packet.decode(bytes);
            return packet;
        } finally {
            bytes.release();
        }
    }

    /** Supplies real queue/configuration state while recording deferred production requests. */
    private static final class TestHandler extends ReliabilityHandler {
        /** Number of packet-production requests made after completion or recall. */
        private int productionRequests;

        /**
         * Gives the handler real defaults without installing unrelated transport timers.
         *
         * @param channel allocator and configuration owner
         */
        private TestHandler(final EmbeddedChannel channel) {
            config = new DefaultConfig(channel);
        }

        /**
         * Transfers one reference-counted frame set into the actual outstanding queue.
         *
         * @param sequence packet sequence number
         * @return frame set whose release the test checks
         */
        private FrameSet pending(final int sequence) {
            final FrameSet frameSet = FrameSet.create();
            frameSet.setSeqId(sequence);
            pendingFrameSets.put(sequence, frameSet);
            return frameSet;
        }

        /** Records the deferred callback without sending new frames during an acknowledgement test. */
        @Override
        protected void tryProduceFrameSets() {
            productionRequests++;
        }
    }
}
