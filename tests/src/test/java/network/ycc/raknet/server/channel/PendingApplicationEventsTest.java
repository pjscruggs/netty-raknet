package network.ycc.raknet.server.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.TooLongFrameException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks deferred-delivery ownership under overflow, cancellation, delivery failure, and successful reuse. */
class PendingApplicationEventsTest {
    /** Payload admission is bounded before ownership transfer, and cancellation releases accepted references. */
    @Test
    void byteLimitAndCancellationPreserveOwnership() {
        final PendingApplicationEvents events = new PendingApplicationEvents();
        final ByteBuf accepted = Unpooled.buffer().writeLong(1);
        final ByteBuf rejected = Unpooled.buffer().writeByte(1);
        try {
            events.add(accepted::release, accepted, 8);
            assertThrows(TooLongFrameException.class, () -> events.add(rejected::release, rejected, 8));
            assertEquals(1, rejected.refCnt());
            events.clear();
            assertEquals(0, accepted.refCnt());
            events.clear();
        } finally {
            events.clear();
            rejected.release();
        }
    }

    /** Lifecycle events without payload bytes still have a finite retention limit. */
    @Test
    void callbackCountBoundsPayloadFreeEvents() {
        final PendingApplicationEvents events = new PendingApplicationEvents();
        final AtomicInteger delivered = new AtomicInteger();
        for (int index = 0; index < 256; index++) {
            events.add(delivered::incrementAndGet, null, 1);
        }
        assertThrows(TooLongFrameException.class, () -> events.add(delivered::incrementAndGet, null, 1));
        events.drain();
        assertEquals(256, delivered.get());
        events.add(delivered::incrementAndGet, null, 1);
        events.drain();
        assertEquals(257, delivered.get());
    }

    /** A rejected callback releases its owned payload and every callback that can no longer be delivered. */
    @Test
    void failedDeliveryReleasesCurrentAndRemainingReferences() {
        final PendingApplicationEvents events = new PendingApplicationEvents();
        final ByteBuf first = Unpooled.buffer().writeByte(1);
        final ByteBuf second = Unpooled.buffer().writeByte(2);
        final IllegalStateException rejected = new IllegalStateException("Application unavailable");
        try {
            events.add(() -> { throw rejected; }, first, 2);
            events.add(second::release, second, 2);
            assertSame(rejected, assertThrows(IllegalStateException.class, events::drain));
            assertEquals(0, first.refCnt());
            assertEquals(0, second.refCnt());
            events.clear();
        } finally {
            events.clear();
        }
    }

    /** Successful delivery returns the byte reservation and passes the existing reference without retaining it. */
    @Test
    void successfulDeliveryReturnsTheByteBudget() {
        final PendingApplicationEvents events = new PendingApplicationEvents();
        final ByteBuf first = Unpooled.buffer().writeInt(1);
        final ByteBuf second = Unpooled.buffer().writeInt(2);
        try {
            events.add(first::release, first, 4);
            events.drain();
            assertEquals(0, first.refCnt());
            events.add(second::release, second, 4);
            events.drain();
            assertEquals(0, second.refCnt());
        } finally {
            events.clear();
        }
    }
}
