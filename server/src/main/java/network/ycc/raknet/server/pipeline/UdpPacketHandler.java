package network.ycc.raknet.server.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.packet.Packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;

/**
 * Decodes one offline packet type directly from UDP datagrams before a reliable connection exists.
 * Subclasses receive a borrowed decoded packet. They must retain it before keeping it after the callback.
 *
 * @param <T> unframed packet type accepted by this handler
 */
public abstract class UdpPacketHandler<T extends Packet> extends
        SimpleChannelInboundHandler<DatagramPacket> {

    /**
     * Offline packet class used to resolve the connection codec's numeric packet identifier.
     */
    private final Class<T> type;
    /**
     * Unsigned first-byte discriminator resolved when the handler enters the pipeline.
     */
    private int packetId;

    /**
     * Selects an offline packet type and rejects framed types that require reliable transport decoding.
     *
     * @param type packet class accepted by the subclass callback
     * @throws IllegalArgumentException if the supplied class implements FramedPacket
     */
    public UdpPacketHandler(final Class<T> type) {
        if (FramedPacket.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(
                    "Framed packet types cannot be directly handled by UdpPacketHandler");
        }
        this.type = type;
    }

    /**
     * Processes a decoded offline packet while its reference is borrowed from the enclosing decoder.
     * The packet is released when this callback returns. Retain it before scheduling asynchronous work.
     *
     * @param ctx listening channel context
     * @param sender remote address from the UDP envelope
     * @param recipient local address from the UDP envelope, which may be null for an internal replay
     * @param packet decoded request borrowed until this callback completes
     */
    protected abstract void handle(ChannelHandlerContext ctx, InetSocketAddress sender, InetSocketAddress recipient, T packet);

    /**
     * Resolves the selected class against this listener's codec before accepting datagrams.
     *
     * @param ctx listener holding the configured packet codec
     * @throws IllegalArgumentException if the codec has no identifier for the selected class
     */
    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) {
        final RakNet.Config config = RakNet.config(ctx);
        packetId = config.getCodec().packetIdFor(type);
        if (packetId == -1) {
            throw new IllegalArgumentException("Unknown packet ID for class " + type);
        }
    }

    /**
     * Checks the datagram's leading byte without advancing its reader index or taking another reference.
     * A datagram must contain at least its discriminator byte before reaching this handler.
     *
     * @param msg candidate inbound message
     * @return whether the message is a datagram with this handler's packet identifier
     */
    @Override
    public boolean acceptInboundMessage(final Object msg) {
        if (msg instanceof DatagramPacket) {
            final ByteBuf content = ((DatagramPacket) msg).content();
            return content.getUnsignedByte(content.readerIndex()) == packetId;
        }
        return false;
    }

    /**
     * Decodes a matched datagram and releases the decoded packet after the subclass callback completes.
     * SimpleChannelInboundHandler separately releases the original datagram reference.
     *
     * @param ctx listener supplying its codec
     * @param msg accepted datagram borrowed from the enclosing inbound handler
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) {
        final RakNet.Config config = RakNet.config(ctx);
        final T packet = (T) config.getCodec().decode(msg.content());
        try {
            handle(ctx, msg.sender(), msg.recipient(), packet);
        } finally {
            ReferenceCountUtil.release(packet);
        }
    }

    /**
     * Exposes the offline packet class selected when this handler was constructed.
     *
     * @return class used for codec lookup and the typed callback
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Exposes the current leading-byte discriminator without consulting the codec again.
     *
     * @return packet identifier resolved on handler installation or replaced explicitly
     */
    public int getPacketId() {
        return packetId;
    }

    /**
     * Replaces the leading-byte discriminator used by subsequent acceptance checks.
     * This does not change the selected packet class or validate the codec's decoded result.
     *
     * @param packetId discriminator to match against the datagram's unsigned first byte
     */
    public void setPacketId(final int packetId) {
        this.packetId = packetId;
    }

}
