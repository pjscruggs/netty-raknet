package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import network.ycc.raknet.frame.Frame;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Bounds aggregate fragment retention and verifies budget recovery through each ownership exit. */
class ReassemblyBudgetTest {
    /** Many individually valid split groups cannot bypass the connection-wide receive ceiling. */
    @Test
    void incompleteGroupsShareOneBudget() {
        final FrameJoiner joiner = new FrameJoiner();
        final EmbeddedChannel channel = new EmbeddedChannel(joiner);
        final Frame first = fragment(1, 0, 2);
        final Frame second = fragment(2, 0, 2);
        final Frame third = fragment(3, 0, 2);
        final Frame rejected = fragment(4, 0, 2);
        final List<Object> output = new ArrayList<>();
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, first, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, second, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, third, output);
            assertThrows(TooLongFrameException.class, () ->
                    joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, rejected, output));
            assertEquals(1, rejected.refCnt());
            assertEquals(2, first.refCnt());
            assertEquals(3, joiner.pendingPackets.size());
            channel.finishAndReleaseAll();
            assertEquals(1, first.refCnt());
            assertEquals(1, second.refCnt());
            assertEquals(1, third.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            first.release();
            second.release();
            third.release();
            rejected.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /** Repeating an already retained fragment neither acquires another reference nor consumes budget. */
    @Test
    void duplicatesDoNotConsumeBudget() {
        final FrameJoiner joiner = new FrameJoiner();
        final EmbeddedChannel channel = new EmbeddedChannel(joiner);
        final Frame first = fragment(1, 0, 2);
        final Frame second = fragment(2, 1, 2);
        final Frame third = fragment(3, 0, 2);
        final List<Object> output = new ArrayList<>();
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, first, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, second, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, first, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, second, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, third, output);
            assertEquals(3, joiner.pendingPackets.size());
            assertEquals(2, first.refCnt());
            assertEquals(2, second.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            first.release();
            second.release();
            third.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /** Completing one group returns its full budget while keeping other groups intact. */
    @Test
    void completionReturnsBudget() {
        final FrameJoiner joiner = new FrameJoiner();
        final EmbeddedChannel channel = new EmbeddedChannel(joiner);
        final Frame first = fragment(1, 0, 2);
        final Frame last = fragment(1, 1, 2);
        final Frame second = fragment(2, 0, 2);
        final Frame third = fragment(3, 0, 2);
        final List<Object> output = new ArrayList<>();
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, first, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, second, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, last, output);
            assertEquals(1, output.size());
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, third, output);
            assertEquals(2, joiner.pendingPackets.size());
            assertEquals(1, first.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            first.release();
            last.release();
            second.release();
            third.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /** Invalidating a group returns its budget, and a lane reset returns every remaining reservation. */
    @Test
    void failedGroupAndResetReturnBudget() {
        final FrameJoiner joiner = new FrameJoiner();
        final EmbeddedChannel channel = new EmbeddedChannel(joiner);
        final Frame first = fragment(1, 0, 2);
        final Frame conflict = fragment(1, 1, 3);
        final Frame second = fragment(2, 0, 2);
        final Frame third = fragment(3, 0, 2);
        final List<Object> output = new ArrayList<>();
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, first, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, second, output);
            assertThrows(CorruptedFrameException.class, () ->
                    joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, conflict, output));
            assertEquals(1, first.refCnt());
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, third, output);
            joiner.discardOrderedChannel(0);
            assertEquals(1, second.refCnt());
            assertEquals(1, third.refCnt());
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, first, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, second, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, third, output);
            assertEquals(3, joiner.pendingPackets.size());
        } finally {
            channel.finishAndReleaseAll();
            first.release();
            conflict.release();
            second.release();
            third.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /** Delayed synchronization cursors release obsolete fragments without destroying newer partial packets. */
    @Test
    void cursorResetPreservesNewerFragmentGroups() {
        final FrameJoiner joiner = new FrameJoiner();
        final EmbeddedChannel channel = new EmbeddedChannel(joiner);
        final Frame old = fragment(4, 0, 2);
        final Frame future = fragment(9, 0, 2);
        final List<Object> output = new ArrayList<>();
        try {
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, old, output);
            joiner.join(UnpooledByteBufAllocator.DEFAULT, 75, future, output);
            joiner.discardOrderedThrough(0, 5);
            assertEquals(1, old.refCnt());
            assertEquals(2, future.refCnt());
            joiner.discardOrderedThrough(0, 3);
            assertEquals(2, future.refCnt());
            assertEquals(1, joiner.pendingPackets.size());
        } finally {
            channel.finishAndReleaseAll();
            old.release();
            future.release();
            output.forEach(ReferenceCountUtil::release);
        }
    }

    /**
     * Decodes a one-byte ordered fragment whose conservative retention cost is twenty-five bytes.
     *
     * @param group independent split identifier
     * @param index fragment position within the packet
     * @param count advertised fragment count, including deliberate metadata conflicts
     * @return decoded frame with one reference owned by the test
     */
    private static Frame fragment(final int group, final int index, final int count) {
        final ByteBuf bytes = Unpooled.buffer();
        try {
            bytes.writeByte(0x70).writeShort(8).writeMediumLE(index);
            bytes.writeMediumLE(group).writeByte(0);
            bytes.writeInt(count).writeShort(group).writeInt(index).writeByte(0xfd);
            return Frame.read(bytes);
        } finally {
            bytes.release();
        }
    }
}
