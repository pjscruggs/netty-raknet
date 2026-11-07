package network.ycc.raknet.server.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayDeque;
import java.util.Queue;
import network.ycc.raknet.frame.FrameData;

/** Owns bounded deliveries while application registration is pending, confined to the transport loop. */
final class PendingApplicationEvents {
    /** Bounds callback objects even when events do not contain measurable payload bytes. */
    private static final int MAX_EVENTS = 256;
    /** Accepted operations in their original transport-event order. */
    private final Queue<Delivery> deliveries = new ArrayDeque<>();
    /** Payload bytes owned by queued operations, excluding callback and container overhead. */
    private long pendingBytes;

    /**
     * Takes ownership only after both the callback-count and payload-byte limits permit admission.
     *
     * @param operation transfers the owned reference into the application pipeline when run
     * @param owned message or event released if registration is abandoned, or null for lifecycle events
     * @param maxBytes transport's configured maximum retained payload bytes
     * @throws TooLongFrameException if accepting this operation would exceed either limit
     */
    void add(final Runnable operation, final Object owned, final int maxBytes) {
        final int bytes = payloadBytes(owned);
        if (deliveries.size() >= MAX_EVENTS || pendingBytes + bytes > maxBytes) {
            throw new TooLongFrameException("Application registration queue limit exceeded");
        }
        deliveries.add(new Delivery(operation, owned, bytes));
        pendingBytes += bytes;
    }

    /**
     * Transfers queued references in order and returns their reservations before invoking each callback.
     * A rejected delivery releases its reference and all remaining references before propagating the error.
     */
    void drain() {
        Delivery delivery;
        while ((delivery = deliveries.poll()) != null) {
            pendingBytes -= delivery.bytes;
            try {
                delivery.operation.run();
            } catch (RuntimeException failure) {
                ReferenceCountUtil.release(delivery.owned);
                clear();
                throw failure;
            }
        }
    }

    /** Releases all references still awaiting registration and restores an empty payload budget. */
    void clear() {
        Delivery delivery;
        while ((delivery = deliveries.poll()) != null) {
            ReferenceCountUtil.release(delivery.owned);
        }
        pendingBytes = 0;
    }

    /**
     * Measures the buffer-backed transport payloads that can cross this bridge.
     * Other control events still consume one slot in the callback-count limit.
     *
     * @param value owned payload, control event, or null for a lifecycle callback
     * @return readable payload bytes, excluding object and allocator overhead
     */
    private static int payloadBytes(final Object value) {
        if (value instanceof ByteBuf) {
            return ((ByteBuf) value).readableBytes();
        }
        if (value instanceof ByteBufHolder) {
            return ((ByteBufHolder) value).content().readableBytes();
        }
        if (value instanceof FrameData) {
            return ((FrameData) value).getDataSize();
        }
        return 0;
    }

    /** Keeps the release reference separate from the callback so abandonment never needs to execute it. */
    private static final class Delivery {
        /** Callback that transfers the owned reference on successful invocation. */
        private final Runnable operation;
        /** Reference released if delivery is abandoned or rejected. */
        private final Object owned;
        /** Payload reservation captured before ownership transfer. */
        private final int bytes;

        /**
         * Records one already-admitted operation without retaining another payload reference.
         *
         * @param operation application delivery callback
         * @param owned reference whose ownership moves into this entry
         * @param bytes payload-byte reservation associated with the entry
         */
        private Delivery(final Runnable operation, final Object owned, final int bytes) {
            this.operation = operation;
            this.owned = owned;
            this.bytes = bytes;
        }
    }
}
