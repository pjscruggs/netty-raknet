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
     * A completed existing group transfers an assembled frame to the next stage.
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
            final int splitID = frame.getSplitId();
            final Builder partial = pendingPackets.get(splitID);
            final int totalSize = frame.getSplitCount() * frame.getRoughPacketSize();
            frame.touch("Is split");
            if (totalSize > RakNet.config(ctx).getMaxQueuedBytes()) {
                throw new TooLongFrameException("Fragmented frame too large");
            } else if (partial == null) {
                Constants.packetLossCheck(frame.getSplitCount(), "frame join elements");
                pendingPackets.put(splitID, Builder.create(ctx.alloc(), frame));
            } else {
                partial.add(frame);
                if (partial.isDone()) {
                    pendingPackets.remove(splitID);
                    list.add(partial.finish());
                }
            }
            Constants.packetLossCheck(pendingPackets.size(), "pending frame joins");
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
         * Allocates the pending-fragment index using the advertised group size as its capacity estimate.
         *
         * @param size expected fragment count used to size the index
         */
        private Builder(final int size) {
            queue = new Int2ObjectOpenHashMap<>(size);
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
            out.init(alloc, frame);
            return out;
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
         * Reliability, channel and ordering consistency with the sample are checked by assertions.
         *
         * @param packet fragment whose original reference is not consumed
         */
        void add(final Frame packet) {
            assert packet.getReliability().equals(samplePacket.getReliability());
            assert packet.getOrderChannel() == samplePacket.getOrderChannel();
            assert packet.getOrderIndex() == samplePacket.getOrderIndex();
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
