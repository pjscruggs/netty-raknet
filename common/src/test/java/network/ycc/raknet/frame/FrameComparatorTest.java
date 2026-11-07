package network.ycc.raknet.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.PriorityQueue;
import java.util.Queue;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.UINT;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Checks scheduler comparator laws without assuming a total order across an ambiguous serial-number cycle. */
class FrameComparatorTest {
    /** Equal priorities must be symmetric and a priority queue must retain every equal-priority frame. */
    @Test
    void equalPrioritiesAndWrappedCountersRespectComparatorContract() {
        final Frame[] frames = {
                frame(false, 0), frame(false, 0), frame(true, UINT.B3.MAX_VALUE),
                frame(true, 0), frame(true, 0), frame(true, 1)};
        try {
            for (final Frame first : frames) {
                for (final Frame second : frames) {
                    assertEquals(-Integer.signum(Frame.COMPARATOR.compare(second, first)),
                            Integer.signum(Frame.COMPARATOR.compare(first, second)));
                }
            }
            assertEquals(0, Frame.COMPARATOR.compare(frames[0], frames[1]));
            assertEquals(0, Frame.COMPARATOR.compare(frames[3], frames[4]));
            final Queue<Frame> queue = new PriorityQueue<>(Frame.COMPARATOR);
            java.util.Collections.addAll(queue, frames);
            assertEquals(6, queue.size());
            assertFalse(queue.remove().getReliability().isReliable);
            assertFalse(queue.remove().getReliability().isReliable);
            assertSame(frames[2], queue.remove());
            assertEquals(0, queue.remove().getReliableIndex());
            assertEquals(0, queue.remove().getReliableIndex());
            assertSame(frames[5], queue.remove());
        } finally {
            for (final Frame frame : frames) {
                frame.release();
            }
        }
    }

    /**
     * Gives each scheduled frame a separately owned payload and a chosen reliable sequence.
     *
     * @param reliable whether this payload participates in acknowledged sequence order
     * @param sequence twenty-four-bit counter used only by reliable frames
     * @return one caller-owned frame reference
     */
    private static Frame frame(final boolean reliable, final int sequence) {
        final ByteBuf bytes = Unpooled.buffer().writeByte(1);
        final FrameData data = FrameData.create(UnpooledByteBufAllocator.DEFAULT, 0xfd, bytes);
        try {
            data.setReliability(reliable ? FramedPacket.Reliability.RELIABLE : FramedPacket.Reliability.UNRELIABLE);
            final Frame frame = Frame.create(data);
            frame.setReliableIndex(sequence);
            return frame;
        } finally {
            data.release();
            bytes.release();
        }
    }
}
