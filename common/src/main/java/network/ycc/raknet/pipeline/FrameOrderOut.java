package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.UINT;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * Assigns wrapped ordering and freshness counters independently for each outgoing lane.
 * This mutable encoder belongs to one connection and must run on its event loop.
 */
public class FrameOrderOut extends MessageToMessageEncoder<FramedPacket> {

    /** Pipeline name for assigning order metadata before fragmentation. */
    public static final String NAME = "rn-order-out";

    /** Next twenty-four-bit order counter for each of the eight independent lanes. */
    protected int[] nextOrderIndex = new int[8];
    /** Next twenty-four-bit freshness counter for sequenced traffic on each lane. */
    protected int[] nextSequenceIndex = new int[8];

    /**
     * Reports the number of independent ordering lanes without exposing their mutable counters.
     *
     * @return supported lane count
     */
    public final int channelCount() {
        return nextOrderIndex.length;
    }

    /**
     * Reads the last assigned order position on the connection event loop without consuming another position.
     *
     * @param channel lane whose already assigned traffic is being superseded
     * @return twenty-four-bit predecessor of the next outbound position
     */
    public final int lastOrderIndex(final int channel) {
        return UINT.B3.plus(nextOrderIndex[channel], -1);
    }

    /**
     * Encodes a packet, assigns counters required by its delivery mode, and transfers a new frame downstream.
     * The temporary payload reference is released after the frame retains it.
     *
     * @param ctx connection supplying codec rules and allocation
     * @param packet outgoing packet consumed by the encoder framework
     * @param list output list taking ownership of the new frame
     */
    @Override
    protected void encode(final ChannelHandlerContext ctx, final FramedPacket packet, final List<Object> list) {
        final RakNet.Config config = RakNet.config(ctx);
        final FrameData data = config.getCodec().encode(packet, ctx.alloc());
        try {
            if (data.getReliability().isOrdered) {
                final int channel = data.getOrderChannel();
                final int sequenceIndex =
                        data.getReliability().isSequenced ? getNextSequenceIndex(channel) : 0;
                list.add(Frame.createOrdered(data, getNextOrderIndex(channel), sequenceIndex));
            } else {
                list.add(Frame.create(data));
            }
        } finally {
            data.release();
        }
    }

    /**
     * Consumes one order position in the selected lane and wraps its next counter to twenty-four bits.
     *
     * @param channel valid lane index from zero through seven
     * @return order position reserved for the packet being encoded
     */
    protected int getNextOrderIndex(final int channel) {
        final int orderIndex = nextOrderIndex[channel];
        nextOrderIndex[channel] = UINT.B3.plus(nextOrderIndex[channel], 1);
        return orderIndex;
    }

    /**
     * Consumes one freshness counter in the selected lane independently of other lanes.
     *
     * @param channel valid lane index from zero through seven
     * @return twenty-four-bit sequencing counter reserved for the outgoing packet
     */
    protected int getNextSequenceIndex(final int channel) {
        final int sequenceIndex = nextSequenceIndex[channel];
        nextSequenceIndex[channel] = UINT.B3.plus(nextSequenceIndex[channel], 1);
        return sequenceIndex;
    }

}
