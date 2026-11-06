package network.ycc.raknet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Random;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks that the datagram fault fixture preserves direction and releases its delayed references. */
class FaultInjectorTest {
    /** A delayed inbound datagram must reach the application, never be sent back to its peer. */
    @Test
    void delayedInboundPacketsKeepTheirDirection() {
        final EndToEndTest.Brutalizer injector = new EndToEndTest.Brutalizer();
        injector.rnd = new Random(1234);
        injector.brutalizeRead = true;
        injector.orderPercent = 1;
        injector.lossPercent = 0;
        injector.dupePercent = 0;
        final ByteBuf delayed = Unpooled.buffer(1).writeByte(1);
        final ByteBuf next = Unpooled.buffer(1).writeByte(2);
        injector.readStash = delayed;
        final EmbeddedChannel channel = new EmbeddedChannel(injector);
        try {
            assertTrue(channel.writeInbound(next));
            assertSame(delayed, channel.readInbound());
            assertSame(next, channel.readInbound());
            assertNull(channel.readOutbound());
            assertNull(injector.readStash);
        } finally {
            channel.finishAndReleaseAll();
            delayed.release();
            next.release();
        }
    }

    /** Removing the fixture releases both delayed directions even when no later datagram arrives. */
    @Test
    void removalReleasesDelayedPackets() {
        final EndToEndTest.Brutalizer injector = new EndToEndTest.Brutalizer();
        final ByteBuf inbound = Unpooled.buffer(1).writeByte(1);
        final ByteBuf outbound = Unpooled.buffer(1).writeByte(2);
        injector.readStash = inbound;
        injector.writeStash = outbound;
        final EmbeddedChannel channel = new EmbeddedChannel(injector);
        channel.finishAndReleaseAll();
        assertEquals(0, inbound.refCnt());
        assertEquals(0, outbound.refCnt());
    }
}
