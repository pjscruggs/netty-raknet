package network.ycc.raknet.pipeline;

import network.ycc.raknet.frame.FrameData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

/**
 * Adapts application ByteBuf messages to one configured framed-packet identifier.
 * Matching reads expose a retained payload view without the identifier. Other packet types
 * retain their framed representation for subsequent transport handlers.
 */
@ChannelHandler.Sharable
public class UserDataCodec extends MessageToMessageCodec<FrameData, ByteBuf> {

    /** Pipeline name for the application ByteBuf adapter above typed transport packets. */
    public static final String NAME = "rn-user-data-codec";

    /** Unsigned packet identifier distinguishing application bytes from other complete framed packets. */
    private final int packetId;

    /**
     * Selects the application identifier prepended on writes and stripped from matching reads.
     *
     * @param packetId unsigned byte reserved for this application payload type
     */
    public UserDataCodec(final int packetId) {
        this.packetId = packetId;
    }

    /**
     * Wraps nonempty application bytes with their packet identifier while Netty releases the accepted input.
     * Empty writes produce no frame. The created payload retains the original byte storage.
     *
     * @param ctx allocator context for the packet identifier and composite
     * @param buf application buffer whose reference is released by the codec framework
     * @param out output list that takes ownership of the retained frame payload
     */
    @Override
    protected void encode(final ChannelHandlerContext ctx, final ByteBuf buf, final List<Object> out) {
        if (buf.isReadable()) {
            out.add(FrameData.create(ctx.alloc(), packetId, buf));
        }
    }

    /**
     * Strips the matching application identifier from a retained view or forwards an unmatched packet intact.
     * Only the returned view's reader index changes. Fragments must have been reassembled before this stage.
     *
     * @param ctx pipeline context for this application adapter
     * @param packet complete payload released by the codec framework after decoding
     * @param out output list taking ownership of an application view or retained unmatched packet
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, final FrameData packet, final List<Object> out) {
        assert !packet.isFragment();
        if (packet.getDataSize() > 0) {
            if (packetId == packet.getPacketId()) {
                out.add(packet.createData().skipBytes(1));
            } else {
                out.add(packet.retain());
            }
        }
    }

}
