package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket.Reliability;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks ordered-frame bounds and reference ownership without opening a network listener. */
class FrameOrderValidationTest {
    /** Negative, first-outside, and maximum wire-byte channels must close and release their input. */
    @Test
    void invalidOrderedAndSequencedChannelsAreRejected() {
        for (Reliability reliability : new Reliability[] {
                Reliability.RELIABLE_ORDERED, Reliability.RELIABLE_SEQUENCED}) {
            for (int channel : new int[] {-1, 8, 255}) {
                reject(reliability, channel);
            }
        }
    }

    /** All eight channels deliver their first packet and release the enclosing frame. */
    @Test
    void validOrderedChannelsStillDeliver() {
        for (int channel = 0; channel < 8; channel++) {
            EmbeddedChannel pipeline = new EmbeddedChannel(new FrameOrderIn());
            Frame frame = frame(Reliability.RELIABLE_ORDERED, channel);
            try {
                assertTrue(pipeline.writeInbound(frame));
                assertEquals(0, frame.refCnt());
                FrameData accepted = pipeline.readInbound();
                assertNotNull(accepted);
                assertEquals(channel, accepted.getOrderChannel());
                accepted.release();
                assertTrue(pipeline.isOpen());
            } finally {
                pipeline.finishAndReleaseAll();
            }
        }
    }

    /**
     * Verifies rejection happens before any invalid-channel payload reaches application code.
     *
     * @param reliability ordered delivery policy under test
     * @param channel invalid ordering channel
     */
    private static void reject(final Reliability reliability, final int channel) {
        EmbeddedChannel pipeline = new EmbeddedChannel(new FrameOrderIn());
        Frame frame = frame(reliability, channel);
        try {
            assertThrows(CorruptedFrameException.class, () -> pipeline.writeInbound(frame));
            assertEquals(0, frame.refCnt());
            assertFalse(pipeline.isOpen());
            assertNull(pipeline.readInbound());
        } finally {
            pipeline.finishAndReleaseAll();
        }
    }

    /**
     * Creates the first ordered frame on a lane with explicit test-owned payload storage.
     *
     * @param reliability ordered or sequenced delivery policy
     * @param channel ordering channel encoded in the frame metadata
     * @return frame whose ownership transfers to the decoder in the calling test
     */
    private static Frame frame(final Reliability reliability, final int channel) {
        ByteBuf bytes = Unpooled.buffer(1).writeByte(42);
        FrameData data = FrameData.create(UnpooledByteBufAllocator.DEFAULT, 0xfd, bytes);
        try {
            data.setReliability(reliability);
            data.setOrderChannel(channel);
            return Frame.createOrdered(data, 0, 0);
        } finally {
            data.release();
            bytes.release();
        }
    }
}
