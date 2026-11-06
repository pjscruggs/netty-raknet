package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import network.ycc.raknet.frame.Frame;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks untrusted wire fragment metadata before it can allocate or retain a reassembly group. */
class FragmentValidationTest {
    /** A one-fragment split is complete on arrival and must not wait indefinitely for another fragment. */
    @Test
    void singleFragmentCompletesImmediately() {
        final FrameJoiner joiner = new FrameJoiner();
        final List<Object> output = new ArrayList<>();
        final Frame fragment = fragment(1, 0, 0);
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, fragment, output);
            assertEquals(1, output.size());
            assertFalse(((Frame) output.get(0)).hasSplit());
            assertTrue(joiner.pendingPackets.isEmpty());
        } finally {
            fragment.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /** A huge advertised count must fail before multiplication overflows or a large map is allocated. */
    @Test
    void oversizedCountCannotOverflowSizeValidation() {
        final FrameJoiner joiner = new FrameJoiner();
        final Frame fragment = fragment(1 << 28, 0, 0);
        try {
            assertThrows(TooLongFrameException.class, () ->
                    joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, fragment, new ArrayList<>()));
            assertTrue(joiner.pendingPackets.isEmpty());
            assertEquals(1, fragment.refCnt());
        } finally {
            fragment.release();
        }
    }

    /** Zero and negative counts, negative indices and indices at the count are invalid wire metadata. */
    @Test
    void invalidFragmentPositionsDoNotRetainPayloads() {
        for (int[] metadata : new int[][] {{0, 0}, {-1, 0}, {2, -1}, {2, 2}}) {
            final FrameJoiner joiner = new FrameJoiner();
            final Frame fragment = fragment(metadata[0], metadata[1], 0);
            try {
                assertThrows(CorruptedFrameException.class, () ->
                        joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, fragment, new ArrayList<>()));
                assertTrue(joiner.pendingPackets.isEmpty());
                assertEquals(1, fragment.refCnt());
            } finally {
                fragment.release();
            }
        }
    }

    /** Conflicting order counters reject and release the entire group even when Java assertions are disabled. */
    @Test
    void inconsistentGroupReleasesEarlierFragments() {
        final FrameJoiner joiner = new FrameJoiner();
        final Frame first = fragment(2, 0, 0);
        final Frame conflicting = fragment(2, 1, 1);
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, first, new ArrayList<>());
            assertEquals(2, first.refCnt());
            assertThrows(CorruptedFrameException.class, () ->
                    joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, conflicting, new ArrayList<>()));
            assertTrue(joiner.pendingPackets.isEmpty());
            assertEquals(1, first.refCnt());
            assertEquals(1, conflicting.refCnt());
        } finally {
            first.release();
            conflicting.release();
        }
    }

    /** Valid reverse-order fragments retain their payloads until the missing prefix arrives, then finish once. */
    @Test
    void reverseOrderFragmentsStillComplete() {
        final FrameJoiner joiner = new FrameJoiner();
        final List<Object> output = new ArrayList<>();
        final Frame last = fragment(2, 1, 0);
        final Frame first = fragment(2, 0, 0);
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, last, output);
            assertTrue(output.isEmpty());
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 1024, first, output);
            assertEquals(1, output.size());
            assertTrue(joiner.pendingPackets.isEmpty());
            assertEquals(1, last.refCnt());
        } finally {
            first.release();
            last.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /**
     * Decodes a reliable ordered split frame from wire bytes, including deliberately invalid metadata.
     *
     * @param count advertised total number of fragments
     * @param index fragment position within the advertised group
     * @param order ordering counter shared by valid members of one group
     * @return decoded frame with one caller-owned reference
     */
    private static Frame fragment(final int count, final int index, final int order) {
        final ByteBuf bytes = Unpooled.buffer();
        try {
            bytes.writeByte(0x70).writeShort(8).writeMediumLE(index);
            bytes.writeMediumLE(order).writeByte(0);
            bytes.writeInt(count).writeShort(1).writeInt(index).writeByte(0xfd);
            return Frame.read(bytes);
        } finally {
            bytes.release();
        }
    }
}
