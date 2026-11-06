package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.utils.UINT;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * Fits outgoing packets to the negotiated MTU and assigns connection-wide reliable counters.
 * Split packets are upgraded to reliable delivery so reassembly can recover every fragment.
 */
public class FrameSplitter extends MessageToMessageEncoder<Frame> {

    /** Pipeline name for fragmentation after ordering metadata has been assigned. */
    public static final String NAME = "rn-split";

    /** Next sixteen-bit identifier shared by the fragments of one outgoing packet. */
    protected int nextSplitId;
    /** Next twenty-four-bit reliable counter, consumed once per reliable frame or fragment. */
    protected int nextReliableId;

    /**
     * Fragments an oversized frame or retains a fitting frame after assigning its reliable counter.
     * The conservative payload budget leaves room for both frame-set and frame headers.
     *
     * @param ctx connection supplying the negotiated MTU
     * @param packet input frame released by the encoder framework after processing
     * @param list output list taking ownership of retained or newly created frames
     */
    @Override
    protected void encode(final ChannelHandlerContext ctx, final Frame packet, final List<Object> list) {
        final RakNet.Config config = RakNet.config(ctx);
        final int maxSize = config.getMTU() - 2 * (FrameSet.HEADER_SIZE + Frame.HEADER_SIZE);
        if (packet.getRoughPacketSize() > maxSize) {
            final int splits = packet.fragment(getNextSplitID(), maxSize, nextReliableId, list);
            nextReliableId = UINT.B3.plus(nextReliableId, splits);
        } else {
            if (packet.getReliability().isReliable) {
                packet.setReliableIndex(getNextReliableId());
            }
            list.add(packet.retain());
        }
    }

    /**
     * Reserves the split identifier for one packet and advances the sixteen-bit counter.
     *
     * @return identifier to share across that packet's fragments
     */
    protected int getNextSplitID() {
        final int splitId = nextSplitId;
        nextSplitId = UINT.B2.plus(nextSplitId, 1);
        return splitId;
    }

    /**
     * Reserves one reliable frame counter and advances it with twenty-four-bit wraparound.
     *
     * @return counter assigned to the next reliable frame
     */
    protected int getNextReliableId() {
        final int reliableIndex = nextReliableId;
        nextReliableId = UINT.B3.plus(nextReliableId, 1);
        return reliableIndex;
    }

}
