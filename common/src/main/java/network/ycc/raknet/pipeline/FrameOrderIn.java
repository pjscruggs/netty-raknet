package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.utils.UINT;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ReferenceCountUtil;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Restores independent ordering lanes after fragment reassembly and releases retained payloads on removal.
 * Counter comparisons use twenty-four-bit circular distance and all mutable queues belong to the connection event loop.
 */
public class FrameOrderIn extends MessageToMessageDecoder<Frame> {

    /** Pipeline name for restoring application packet order after fragment reassembly. */
    public static final String NAME = "rn-order-in";

    /** Eight independent lane queues, each with its own delivered and sequenced counter state. */
    protected final OrderedChannelPacketQueue[] channels = new OrderedChannelPacketQueue[8];

    /** Creates every ordering lane with no delivered packets and the initial minus-one counter sentinel. */
    public FrameOrderIn() {
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new OrderedChannelPacketQueue();
        }
    }

    /**
     * Abandons queued positions through a validated synchronization cursor on the connection event loop.
     * Later queued positions remain owned by the lane and stale cursors cannot rewind delivery.
     *
     * @param channel lane to advance, from zero through seven
     * @param lastOrderIndex final superseded twenty-four-bit position
     * @throws IllegalArgumentException if the lane is unsupported or the cursor is not a twenty-four-bit value
     */
    public final void skipOrderedThrough(final int channel, final int lastOrderIndex) {
        if (channel < 0 || channel >= channels.length || !UINT.fits(lastOrderIndex, 24)) {
            throw new IllegalArgumentException("Invalid ordering lane or cursor");
        }
        channels[channel].skipTo(UINT.B3.plus(lastOrderIndex, 1));
    }

    /**
     * Delivers consecutive queued payloads made ready by a cursor reset without waiting for another packet.
     * Call after publishing the application's phase-change event on the connection event loop.
     *
     * @param output consumer taking each payload's existing reference for downstream delivery
     */
    public final void drainReadyFrames(final Consumer<FrameData> output) {
        for (final OrderedChannelPacketQueue channel : channels) {
            channel.drainReady(output);
        }
    }

    /**
     * Releases payloads retained behind missing ordering positions when this connection stage is removed.
     *
     * @param ctx pipeline context being detached
     * @throws Exception if the decoder superclass cannot complete its removal callback
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        Arrays.stream(channels).forEach(OrderedChannelPacketQueue::clear);
    }

    /**
     * Routes a frame only after validating the peer-controlled ordering channel.
     *
     * @param ctx connection closed when a frame selects an unsupported channel
     * @param frame input released by the decoder after successful or failed processing
     * @param list accepted payload references transferred to the next handler
     * @throws CorruptedFrameException if a frame selects a channel outside the protocol
     * @throws TooLongFrameException if ordered traffic exceeds the combined receive budget
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, final Frame frame, final List<Object> list) {
        final OrderedChannelPacketQueue lane = requireLane(ctx, frame.getOrderChannel());
        if (frame.getReliability().isSequenced) {
            frame.touch("Sequenced");
            lane.decodeSequenced(frame, list);
        } else if (frame.getReliability().isOrdered) {
            frame.touch("Ordered");
            lane.decodeOrdered(frame, list);
        } else {
            frame.touch("No order");
            list.add(frame.retainedFrameData());
        }
        if (frame.getReliability().isOrdered) {
            checkReceiveBudget(ctx);
        }
    }

    /**
     * Resolves a wire-supplied lane only inside its valid range, before any queue state is accessed.
     * Invalid frames close the connection and remain owned by the enclosing decoder's cleanup.
     *
     * @param ctx connection closed on an unsupported lane
     * @param channel untrusted ordering lane from the frame header
     * @return existing queue for a supported ordering lane
     * @throws CorruptedFrameException if the lane falls outside this connection's queue array
     */
    private OrderedChannelPacketQueue requireLane(final ChannelHandlerContext ctx, final int channel) {
        if (channel >= 0 && channel < channels.length) {
            return channels[channel];
        }
        ctx.close();
        throw new CorruptedFrameException("Invalid RakNet ordering channel: " + channel);
    }

    /**
     * Bounds retained payloads across every lane, rather than allowing each lane a separate allowance.
     * The check reads only eight counters. On overflow all queued ownership is released before the
     * connection closes, including data held behind gaps on otherwise unrelated lanes.
     *
     * @param ctx connection whose configured receive ceiling governs all ordered lanes
     * @throws TooLongFrameException if the combined queue exceeds the configured byte ceiling
     */
    private void checkReceiveBudget(final ChannelHandlerContext ctx) {
        long total = 0;
        for (final OrderedChannelPacketQueue channel : channels) {
            total += channel.queuedBytes;
        }
        if (total > 0 && total > RakNet.config(ctx).getMaxQueuedBytes()) {
            Arrays.stream(channels).forEach(OrderedChannelPacketQueue::clear);
            ctx.close().addListener(RakNet.INTERNAL_WRITE_LISTENER);
            throw new TooLongFrameException("Ordered receive queues exceed the connection budget");
        }
    }

    /** Holds future payloads and delivery counters for one ordering lane on the connection event loop. */
    protected static class OrderedChannelPacketQueue {

        /** Owned payload references indexed by future order counters, transferred onward only when eligible. */
        protected final Int2ObjectOpenHashMap<FrameData> queue = new Int2ObjectOpenHashMap<>();
        /** Last delivered or skipped order counter, initially minus one so zero is the first expected packet. */
        protected int lastOrderIndex = -1;
        /** Newest accepted sequencing counter, initially minus one before any freshness sample arrives. */
        protected int lastSequenceIndex = -1;
        /** Queued payload bytes plus one maximum frame-header allowance for each retained position. */
        private long queuedBytes;

        /**
         * Advances the lane past a gap when a newer sequencing counter arrives, then applies ordinary ordered delivery.
         * Superseded queued entries are released in work proportional to the queue size, not the wire-counter gap.
         *
         * @param frame sequenced input whose original ownership remains with the outer decoder
         * @param list destination for retained payloads made deliverable by this frame
         */
        protected void decodeSequenced(final Frame frame, final List<Object> list) {
            if (UINT.B3.minusWrap(frame.getSequenceIndex(), lastSequenceIndex) > 0) {
                lastSequenceIndex = frame.getSequenceIndex();
                skipTo(frame.getOrderIndex());
            }
            decodeOrdered(frame, list); //register packet as normal
        }

        /**
         * Releases queued positions strictly before the new frame and advances to its immediate predecessor.
         * Circular distances are measured from the previous delivered position so wraparound retains later frames.
         *
         * @param orderIndex incoming frame's twenty-four-bit ordering counter
         */
        private void skipTo(final int orderIndex) {
            final int distance = UINT.B3.minusWrap(orderIndex, lastOrderIndex);
            if (distance <= 1) {
                return;
            }
            final ObjectIterator<Int2ObjectMap.Entry<FrameData>> iterator =
                    queue.int2ObjectEntrySet().fastIterator();
            while (iterator.hasNext()) {
                final Int2ObjectMap.Entry<FrameData> entry = iterator.next();
                final int queuedDistance = UINT.B3.minusWrap(entry.getIntKey(), lastOrderIndex);
                if (queuedDistance > 0 && queuedDistance < distance) {
                    queuedBytes -= entry.getValue().getDataSize() + Frame.HEADER_SIZE;
                    ReferenceCountUtil.release(entry.getValue());
                    iterator.remove();
                }
            }
            lastOrderIndex = UINT.B3.plus(orderIndex, -1);
        }

        /**
         * Delivers the next expected payload and any contiguous queued successors, or retains a new future payload.
         * Already delivered counters and duplicate queued counters produce no additional payload reference.
         *
         * @param frame input whose payload is retained only if queued or emitted
         * @param list destination taking ownership of payloads delivered in lane order
         */
        protected void decodeOrdered(final Frame frame, final List<Object> list) {
            final int indexDiff = UINT.B3.minusWrap(frame.getOrderIndex(), lastOrderIndex);
            if (indexDiff == 1) { //got next packet in line
                FrameData data = frame.retainedFrameData();
                do { //process this packet, and any queued packets following in sequence
                    list.add(data);
                    lastOrderIndex = UINT.B3.plus(lastOrderIndex, 1);
                    data = take(UINT.B3.plus(lastOrderIndex, 1));
                } while (data != null);
            } else if (indexDiff > 1 && !queue.containsKey(frame.getOrderIndex())) {
                // only new future data goes in the queue
                enqueue(frame.getOrderIndex(), frame.retainedFrameData());
            }
        }

        /**
         * Takes ownership of a new future payload and accounts for its retained position.
         * Callers must first rule out duplicate positions and already delivered counters.
         *
         * @param index new future ordering position
         * @param data owned payload reference transferred into this lane
         */
        final void enqueue(final int index, final FrameData data) {
            queue.put(index, data);
            queuedBytes += data.getDataSize() + Frame.HEADER_SIZE;
        }

        /**
         * Transfers one queued reference out of the lane and returns its budget at the same time.
         *
         * @param index position made deliverable by the preceding packet
         * @return owned payload, or null when a gap still prevents consecutive delivery
         */
        private FrameData take(final int index) {
            final FrameData data = queue.remove(index);
            if (data != null) {
                queuedBytes -= data.getDataSize() + Frame.HEADER_SIZE;
            }
            return data;
        }

        /**
         * Advances across consecutive queued positions, returning each reservation before delivery.
         *
         * @param output consumer taking ownership of every newly deliverable payload
         */
        private void drainReady(final Consumer<FrameData> output) {
            FrameData data;
            while ((data = take(UINT.B3.plus(lastOrderIndex, 1))) != null) {
                lastOrderIndex = UINT.B3.plus(lastOrderIndex, 1);
                output.accept(data);
            }
        }

        /** Releases every retained queued payload without changing the lane's counter history. */
        protected void clear() {
            queue.values().forEach(ReferenceCountUtil::release);
            queue.clear();
            queuedBytes = 0;
        }

    }

}
