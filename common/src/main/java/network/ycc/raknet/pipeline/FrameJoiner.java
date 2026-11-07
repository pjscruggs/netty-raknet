package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.utils.Constants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Retains out-of-order fragments until a packet can be assembled on its connection event loop.
 * Unsplit frames pass through with a retained reference. Removing the handler releases all incomplete groups.
 */
public class FrameJoiner extends MessageToMessageDecoder<Frame> {

    /** Pipeline name for fragment reassembly before ordered payload delivery. */
    public static final String NAME = "rn-join";

    /** Incomplete split groups keyed by split identifier, owning all buffered fragment references. */
    protected final Int2ObjectOpenHashMap<Builder> pendingPackets = new Int2ObjectOpenHashMap<>();

    /**
     * Releases incomplete ordered packets in a superseded lane on the connection event loop.
     * Other lanes and unordered fragments retain their original ownership and assembly progress.
     *
     * @param channel ordering lane whose partial packets can no longer be delivered
     */
    public final void discardOrderedChannel(final int channel) {
        final java.util.Iterator<Builder> iterator = pendingPackets.values().iterator();
        while (iterator.hasNext()) {
            final Builder builder = iterator.next();
            final Frame sample = builder.samplePacket;
            if (sample.getReliability().isOrdered && sample.getOrderChannel() == channel) {
                iterator.remove();
                builder.release();
            }
        }
    }

    /**
     * Releases every incomplete packet's sample frame, assembled bytes and still-queued fragments.
     *
     * @param ctx pipeline context being detached
     * @throws Exception if the decoder superclass cannot complete removal
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        pendingPackets.values().forEach(Builder::release);
        pendingPackets.clear();
    }

    /**
     * Routes an unsplit frame directly or retains its fragment in the corresponding reassembly group.
     * A completed group transfers an assembled frame to the next stage, including a one-fragment group.
     *
     * @param ctx connection providing allocation and the configured queued-byte ceiling
     * @param frame input frame released by the decoder framework after processing
     * @param list output list taking ownership of retained or reassembled frames
     * @throws TooLongFrameException if the estimated fragmented size exceeds the configured ceiling
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, final Frame frame, final List<Object> list) {
        if (!frame.hasSplit()) {
            frame.touch("Not split");
            list.add(frame.retain());
        } else {
            join(ctx.alloc(), RakNet.config(ctx).getMaxQueuedBytes(), frame, list);
        }
    }

    /**
     * Validates peer-controlled fragment bounds before allocating or extending a reassembly group.
     * A rejected group releases all accumulated references so malformed metadata cannot strand its buffers.
     *
     * @param allocator allocator for assembled payloads
     * @param maxBytes configured maximum estimated size of one assembled packet
     * @param frame borrowed input fragment, retained only while its group is incomplete
     * @param output destination taking ownership of completed frames
     * @throws CorruptedFrameException if fragment indices or group metadata are inconsistent
     * @throws TooLongFrameException if the size estimate exceeds the configured byte ceiling
     */
    void join(final ByteBufAllocator allocator, final int maxBytes,
            final Frame frame, final List<Object> output) {
        final int splitID = frame.getSplitId();
        Builder partial = pendingPackets.get(splitID);
        try {
            validateFragment(frame, maxBytes);
            if (partial == null) {
                partial = Builder.create(allocator, frame);
            } else {
                partial.add(frame);
            }
            if (partial.isDone()) {
                pendingPackets.remove(splitID);
                output.add(partial.finish());
            } else {
                pendingPackets.put(splitID, partial);
            }
        } catch (RuntimeException exception) {
            pendingPackets.remove(splitID);
            if (partial != null) {
                partial.release();
            }
            throw exception;
        }
    }

    /**
     * Checks split indices and computes the size estimate in a wide integer before any capacity allocation.
     *
     * @param frame fragment whose wire metadata is being checked
     * @param maxBytes configured per-packet byte ceiling
     * @throws CorruptedFrameException if the count, index or delivery mode cannot describe a split packet
     * @throws TooLongFrameException if the full-group estimate exceeds the configured ceiling
     */
    private static void validateFragment(final Frame frame, final int maxBytes) {
        if (frame.getSplitCount() <= 0 || frame.getSplitIndex() < 0
                || frame.getSplitIndex() >= frame.getSplitCount() || !frame.getReliability().isReliable) {
            throw new CorruptedFrameException("Invalid RakNet fragment metadata");
        }
        final long totalSize = (long) frame.getSplitCount() * frame.getRoughPacketSize();
        if (totalSize > maxBytes) {
            throw new TooLongFrameException("Fragmented frame too large");
        }
    }

    /** Owns one split group's sample metadata, contiguous composite bytes and fragments waiting behind a gap. */
    protected static final class Builder {

        /** Owned fragment views indexed by split position until they can be appended contiguously. */
        protected final Int2ObjectOpenHashMap<ByteBuf> queue;
        /** Retained first-arriving frame supplying the final packet's metadata and advertised fragment count. */
        protected Frame samplePacket;
        /** Owned composite containing the contiguous prefix of fragment payloads. */
        protected CompositeByteBuf data;
        /** Next fragment position needed to extend the contiguous assembled prefix. */
        protected int splitIdx;
        /** Ordering channel captured from the sample for reassembly-state inspection. */
        protected int orderId;
        /** Delivery mode captured from the sample for reassembly-state inspection. */
        protected FrameData.Reliability reliability;

        /**
         * Starts the pending-fragment index small so a peer cannot force allocation for fragments not yet received.
         *
         * @param size expected fragment count used to size the index
         */
        private Builder(final int size) {
            queue = new Int2ObjectOpenHashMap<>(Math.min(size, 16));
        }

        /**
         * Initializes a split group and retains its first-arriving frame and fragment data.
         *
         * @param alloc allocator for assembled composite storage
         * @param frame sample fragment whose original reference remains with the caller
         * @return builder owning its retained sample and payload references
         */
        private static Builder create(final ByteBufAllocator alloc, final Frame frame) {
            final Builder out = new Builder(frame.getSplitCount());
            try {
                out.init(alloc, frame);
                return out;
            } catch (RuntimeException exception) {
                out.release();
                throw exception;
            }
        }

        /**
         * Starts a reassembly group with empty contiguous data and inserts the first-arriving fragment.
         * The sample frame stays retained until the group is finished or explicitly released.
         *
         * @param alloc allocator for the composite payload
         * @param packet sample fragment defining the expected group metadata
         */
        void init(final ByteBufAllocator alloc, final Frame packet) {
            assert data == null;
            splitIdx = 0;
            data = alloc.compositeDirectBuffer(packet.getSplitCount());
            orderId = packet.getOrderChannel();
            reliability = packet.getReliability();
            samplePacket = packet.retain();
            add(packet);
        }

        /**
         * Retains a previously unseen fragment at or after the next needed position, then extends contiguous data.
         * Group size, delivery mode, channel and ordering counters must match the retained sample.
         *
         * @param packet fragment whose original reference is not consumed
         * @throws CorruptedFrameException if the fragment disagrees with the group's delivery metadata
         */
        void add(final Frame packet) {
            if (packet.getReliability() != samplePacket.getReliability()
                    || packet.getOrderChannel() != samplePacket.getOrderChannel()
                    || packet.getOrderIndex() != samplePacket.getOrderIndex()
                    || packet.getSequenceIndex() != samplePacket.getSequenceIndex()
                    || packet.getSplitCount() != samplePacket.getSplitCount()) {
                throw new CorruptedFrameException("Inconsistent RakNet fragment group");
            }
            if (!queue.containsKey(packet.getSplitIndex()) && packet.getSplitIndex() >= splitIdx) {
                queue.put(packet.getSplitIndex(), packet.retainedFragmentData());
                update();
            }
            Constants.packetLossCheck(queue.size(), "packet defragment queue");
        }

        /** Transfers consecutively available fragment references from the pending index into the composite. */
        void update() {
            ByteBuf fragment;
            while ((fragment = queue.remove(splitIdx)) != null) {
                data.addComponent(true, fragment);
                splitIdx++;
            }
        }

        /**
         * Creates an unsplit frame retaining the assembled bytes, then releases every reference owned by this builder.
         * The caller must invoke this only after all advertised fragments have been assembled.
         *
         * @return complete frame with one caller-owned reference
         */
        Frame finish() {
            assert isDone();
            assert queue.isEmpty();
            try {
                return samplePacket.completeFragment(data);
            } finally {
                release();
            }
        }

        /**
         * Compares the contiguous assembled fragment count with the count advertised by the sample.
         *
         * @return whether the advertised final fragment position has been assembled
         */
        boolean isDone() {
            assert samplePacket.getSplitCount() >= splitIdx;
            return samplePacket.getSplitCount() == splitIdx;
        }

        /** Releases assembled storage, the sample frame and queued fragments, then clears their ownership state. */
        void release() {
            if (data != null) {
                data.release();
                data = null;
            }
            if (samplePacket != null) {
                samplePacket.release();
                samplePacket = null;
            }
            queue.values().forEach(ReferenceCountUtil::release);
            queue.clear();
        }

    }

}
