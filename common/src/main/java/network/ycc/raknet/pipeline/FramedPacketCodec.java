package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

/**
 * Converts between retained frame payloads and typed framed packets using connection-local codec rules.
 * No mutable state is kept in the handler, allowing the same instance in multiple pipelines.
 */
@ChannelHandler.Sharable
public class FramedPacketCodec extends MessageToMessageCodec<FrameData, FramedPacket> {

    /** Pipeline name for the typed packet layer above frame ordering and reassembly. */
    public static final String NAME = "rn-framed-codec";
    /** Shared codec instance whose behavior is selected through each connection's configuration. */
    public static final FramedPacketCodec INSTANCE = new FramedPacketCodec();

    /**
     * Encodes a typed packet into an owned frame payload before Netty releases the accepted input.
     *
     * @param ctx connection supplying the codec and allocator
     * @param in typed packet accepted by this encoder
     * @param out output list taking ownership of the produced payload
     */
    @Override
    protected void encode(final ChannelHandlerContext ctx, final FramedPacket in, final List<Object> out) {
        out.add(RakNet.config(ctx).getCodec().encode(in, ctx.alloc()));
    }

    /**
     * Decodes a complete payload and transfers the resulting packet to the next handler.
     * The configured codec must retain any bytes needed after Netty releases the input payload.
     *
     * @param ctx connection supplying the framed packet decoder
     * @param in complete payload released by the codec framework after this callback
     * @param out output list taking ownership of the decoded result
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, final FrameData in, final List<Object> out) {
        out.add(RakNet.config(ctx).getCodec().decode(in));
    }

}
