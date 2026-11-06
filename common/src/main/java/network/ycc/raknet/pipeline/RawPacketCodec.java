package network.ycc.raknet.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.Packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

/**
 * Converts complete datagram bytes to protocol packets using each connection's configured codec.
 * The handler is stateless and shareable. Netty releases accepted inputs after each conversion.
 */
@ChannelHandler.Sharable
public class RawPacketCodec extends MessageToMessageCodec<ByteBuf, Packet> {

    /** Pipeline name used to place raw packet conversion around datagram transport handlers. */
    public static final String NAME = "rn-raw-codec";
    /** Shared stateless codec instance that reads connection-specific configuration from its context. */
    public static final RawPacketCodec INSTANCE = new RawPacketCodec();

    /**
     * Encodes a protocol packet and records its encoded size before handing the owned buffer downstream.
     *
     * @param ctx connection supplying the packet codec, allocator and byte counters
     * @param in packet released by the codec framework after encoding when reference counted
     * @param out output list that takes ownership of the encoded buffer
     */
    @Override
    protected void encode(final ChannelHandlerContext ctx, final Packet in, final List<Object> out) {
        final ByteBuf encoded = RakNet.config(ctx).getCodec().produceEncoded(in, ctx.alloc());
        RakNet.metrics(ctx).bytesOut(encoded.readableBytes());
        out.add(encoded);
    }

    /**
     * Decodes a nonempty datagram and counts malformed-frame errors without forwarding their contents.
     * CorruptedFrameException is consumed so reliable traffic can be retransmitted. Other failures propagate.
     *
     * @param ctx connection supplying the decoder and receive metrics
     * @param in datagram released by the codec framework after decoding
     * @param out output list receiving the decoded packet and its ownership
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
        if (in.readableBytes() != 0) {
            RakNet.metrics(ctx).bytesIn(in.readableBytes());
            try {
                out.add(RakNet.config(ctx).getCodec().decode(in));
            } catch (CorruptedFrameException e) {
                RakNet.metrics(ctx).frameError(1); //tolerate frame errors, they'll get resent.
            }
        }
    }

}
