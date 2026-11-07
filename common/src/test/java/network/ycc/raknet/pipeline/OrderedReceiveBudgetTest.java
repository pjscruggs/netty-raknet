package network.ycc.raknet.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import network.ycc.raknet.config.DefaultConfig;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises receive limits through actual decoder ownership and independent ordering lanes. */
class OrderedReceiveBudgetTest {
    /** Overflow closes the connection and releases every lane, while a duplicate consumes no budget. */
    @Test
    void lanesShareOneReceiveBudget() {
        final BudgetChannel channel = new BudgetChannel();
        channel.config().setMaxQueuedBytes(52);
        final Frame first = frame(0, 1);
        final Frame second = frame(1, 1);
        final FrameData firstData = first.retainedFrameData();
        final FrameData secondData = second.retainedFrameData();
        try {
            assertFalse(channel.writeInbound(first));
            assertFalse(channel.writeInbound(second));
            assertFalse(channel.writeInbound(frame(0, 1)));
            assertTrue(channel.isOpen());
            assertThrows(TooLongFrameException.class, () -> channel.writeInbound(frame(2, 1)));
            assertFalse(channel.isOpen());
            assertEquals(1, firstData.refCnt());
            assertEquals(1, secondData.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            firstData.release();
            secondData.release();
        }
    }

    /** Receiving a missing prefix drains its queued successor and makes that budget reusable. */
    @Test
    void deliveryReturnsReceiveBudget() {
        final BudgetChannel channel = new BudgetChannel();
        channel.config().setMaxQueuedBytes(26);
        try {
            assertFalse(channel.writeInbound(frame(0, 1)));
            assertTrue(channel.writeInbound(frame(0, 0)));
            final FrameData first = channel.readInbound();
            final FrameData second = channel.readInbound();
            first.release();
            second.release();
            assertFalse(channel.writeInbound(frame(1, 1)));
            assertTrue(channel.isOpen());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /** A phase reset releases skipped payloads and returns their allowance for another lane. */
    @Test
    void synchronizationReturnsReceiveBudget() {
        final BudgetChannel channel = new BudgetChannel();
        channel.config().setMaxQueuedBytes(26);
        try {
            assertFalse(channel.writeInbound(frame(0, 1)));
            channel.pipeline().get(FrameOrderIn.class).skipOrderedThrough(0, 1);
            assertFalse(channel.writeInbound(frame(1, 1)));
            assertTrue(channel.isOpen());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Builds a two-byte complete packet whose queued cost includes the twenty-four-byte header allowance.
     *
     * @param lane independent ordered stream selected by the peer
     * @param order position used to create or fill a receive gap
     * @return frame whose sole reference transfers into the decoder
     */
    private static Frame frame(final int lane, final int order) {
        final ByteBuf bytes = Unpooled.buffer().writeByte(42);
        final FrameData data = FrameData.create(bytes.alloc(), 0xfd, bytes);
        try {
            data.setOrderChannel(lane);
            return Frame.createOrdered(data, order, 0);
        } finally {
            data.release();
            bytes.release();
        }
    }

    /** Supplies real transport configuration to the decoder without sockets or asynchronous timers. */
    private static final class BudgetChannel extends EmbeddedChannel {
        /** Lazily initialized because the superclass can request configuration during construction. */
        private DefaultConfig transportConfig;

        /** Installs the production ordering decoder on this isolated event loop. */
        private BudgetChannel() {
            super(new FrameOrderIn());
        }

        /**
         * Returns persistent RakNet settings whose queue budget is selected by each test.
         *
         * @return this channel's transport configuration
         */
        @Override
        public DefaultConfig config() {
            if (transportConfig == null) {
                transportConfig = new DefaultConfig(this);
            }
            return transportConfig;
        }
    }
}
