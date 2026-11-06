package network.ycc.raknet.server.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.ConnectionRequest1;
import network.ycc.raknet.packet.InvalidVersion;
import network.ycc.raknet.packet.Packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;

/**
 * Accepts supported offline probes and replays them into the newly created peer transport pipeline.
 */
public class ConnectionListener extends UdpPacketHandler<ConnectionRequest1> {

    /**
     * Pipeline name of the listener's offline connection-admission handler.
     */
    public static final String NAME = "rn-connect-init";

    /**
     * Restricts offline decoding to the initial MTU and protocol-version probe.
     */
    public ConnectionListener() {
        super(ConnectionRequest1.class);
    }

    /**
     * Creates or finds a peer transport for a supported version, otherwise sends an invalid-version reply.
     * A supported request receives an extra reference until asynchronous child creation and replay finish.
     * The enclosing decoder remains responsible for its original request reference.
     *
     * @param ctx listening server context used for child creation and replies
     * @param sender remote client address identifying the child connection
     * @param recipient local address on which this probe arrived
     * @param request decoded initial probe borrowed for the duration of this callback
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void handle(final ChannelHandlerContext ctx, final InetSocketAddress sender, final InetSocketAddress recipient, final ConnectionRequest1 request) {
        final RakNet.Config config = RakNet.config(ctx);
        if (config.containsProtocolVersion(request.getProtocolVersion())) {
            ReferenceCountUtil.retain(request);
            //use connect to create a new child for this remote address
            ctx.channel().connect(sender, recipient).addListeners(
                    ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE,
                    future -> {
                        if (future.isSuccess()) {
                            resendRequest(ctx, sender, request);
                        } else {
                            ReferenceCountUtil.safeRelease(request);
                        }
                    }
            );
        } else {
            sendResponse(ctx, sender, new InvalidVersion(config.getMagic(), config.getServerId()));
        }
    }

    /**
     * Encodes an offline reply, transfers a retained buffer into the datagram write, and releases local ownership.
     * The supplied packet is consumed even if encoding fails.
     *
     * @param ctx listener providing the codec, allocator and outbound pipeline
     * @param sender client address receiving the reply
     * @param packet response whose reference is released by this method
     */
    protected void sendResponse(final ChannelHandlerContext ctx, final InetSocketAddress sender, final Packet packet) {
        final RakNet.Config config = RakNet.config(ctx);
        final ByteBuf buf = ctx.alloc().ioBuffer(packet.sizeHint());
        try {
            config.getCodec().encode(packet, buf);
            ctx.writeAndFlush(new DatagramPacket(buf.retain(), sender))
                    .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        } finally {
            ReferenceCountUtil.safeRelease(packet);
            buf.release();
        }
    }

    /**
     * Re-encodes a retained probe and injects it at the listener head for routing to its new child.
     * The synthetic datagram has a null recipient so it cannot trigger another connection-creation pass.
     * This method consumes the request reference retained before asynchronous child creation.
     *
     * @param ctx listener pipeline through which the replay is routed
     * @param sender remote address selecting the newly registered child
     * @param request retained initial probe consumed by this replay
     */
    protected void resendRequest(final ChannelHandlerContext ctx, final InetSocketAddress sender, final ConnectionRequest1 request) {
        final RakNet.Config config = RakNet.config(ctx);
        final ByteBuf buf = ctx.alloc().ioBuffer(request.sizeHint());
        try {
            config.getCodec().encode(request, buf);
            ctx.pipeline().fireChannelRead(new DatagramPacket(buf.retain(),
                    null, sender)).fireChannelReadComplete();
        } finally {
            ReferenceCountUtil.safeRelease(request);
            buf.release();
        }
    }

}
