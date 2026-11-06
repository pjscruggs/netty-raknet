package network.ycc.raknet.server.pipeline;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.ClientHandshake;
import network.ycc.raknet.packet.ConnectionFailed;
import network.ycc.raknet.packet.ConnectionReply1;
import network.ycc.raknet.packet.ConnectionReply2;
import network.ycc.raknet.packet.ConnectionRequest;
import network.ycc.raknet.packet.ConnectionRequest1;
import network.ycc.raknet.packet.ConnectionRequest2;
import network.ycc.raknet.packet.InvalidVersion;
import network.ycc.raknet.packet.Packet;
import network.ycc.raknet.packet.ServerHandshake;
import network.ycc.raknet.pipeline.AbstractConnectionInitializer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;

/** Negotiates server-side RakNet transport settings before handing the connection to the application. */
public class ConnectionInitializer extends AbstractConnectionInitializer {

    /** Whether a client identifier has been bound to this handshake and must remain unchanged. */
    protected boolean clientIdSet;
    /** Whether the operator supplied an MTU that probe packets must not replace. */
    protected boolean mtuFixed;
    /** Whether the first probe arrived, allowing a phase-one reply to be sent. */
    protected boolean seenFirst;

    /**
     * Associates handshake success or failure with the caller's connection promise.
     *
     * @param connectPromise completed when application traffic can begin or negotiation fails
     */
    public ConnectionInitializer(final ChannelPromise connectPromise) {
        super(connectPromise);
    }

    /**
     * Pins the MTU on the owning event loop so incoming probes cannot overwrite the supplied value.
     *
     * @param channel connection whose initializer is still installed
     * @param mtu maximum datagram size selected by the operator
     */
    public static void setFixedMTU(final Channel channel, final int mtu) {
        channel.eventLoop().execute(() -> {
            channel.pipeline().get(ConnectionInitializer.class).mtuFixed = true;
            RakNet.config(channel).setMTU(mtu);
        });
    }

    /**
     * Validates the peer identity and advances only the handshake phase matching the received packet.
     *
     * @param ctx connection pipeline used for replies, timers and application handoff
     * @param msg decoded handshake packet
     * @throws IllegalStateException if the peer rejects the connection or the phase is invalid
     */
    @SuppressWarnings("unchecked")
    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final Packet msg) {
        final RakNet.Config config = RakNet.config(ctx);
        if (msg instanceof Packet.ClientIdConnection) {
            processClientId(ctx, ((Packet.ClientIdConnection) msg).getClientId());
        } else if (msg instanceof ConnectionFailed) {
            throw new IllegalStateException("Connection failed");
        }
        switch (state) {
            case CR1:
                if (!readUnreliableRequest(ctx, msg, config)) {
                    return;
                }
                break;
            case CR2: {
                if (msg instanceof ConnectionRequest) {
                    final ConnectionRequest cr = (ConnectionRequest) msg;
                    final Packet packet = new ServerHandshake(
                            (InetSocketAddress) ctx.channel().remoteAddress(),
                            cr.getTimestamp());
                    ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                    state = State.CR3;
                    startPing(ctx);
                }
                break;
            }
            case CR3: {
                if (msg instanceof ClientHandshake) {
                    finish(ctx);
                    return;
                }
                break;
            }
            default:
                throw new IllegalStateException("Unknown state " + state);
        }

        sendRequest(ctx);
    }

    /**
     * Validates offline magic and MTU probes before reliable handshake traffic starts.
     *
     * @param ctx connection on which an unsupported-version reply closes the channel
     * @param msg initial probe or follow-up connection request
     * @param config connection-local protocol and negotiated transport settings
     * @return whether the caller should send the current phase's reply
     */
    private boolean readUnreliableRequest(final ChannelHandlerContext ctx, final Packet msg,
            final RakNet.Config config) {
        if (msg instanceof ConnectionRequest1) {
            final ConnectionRequest1 request = (ConnectionRequest1) msg;
            request.getMagic().verify(config.getMagic());
            if (!mtuFixed) {
                config.setMTU(request.getMtu());
            }
            seenFirst = true;
            if (!config.containsProtocolVersion(request.getProtocolVersion())) {
                final InvalidVersion packet = new InvalidVersion(config.getMagic(), config.getServerId());
                ctx.writeAndFlush(packet).addListener(ChannelFutureListener.CLOSE);
                return false;
            }
            config.setProtocolVersion(request.getProtocolVersion());
        } else if (msg instanceof ConnectionRequest2) {
            final ConnectionRequest2 request = (ConnectionRequest2) msg;
            request.getMagic().verify(config.getMagic());
            if (!mtuFixed) {
                config.setMTU(request.getMtu());
            }
            state = State.CR2;
        }
        return true;
    }

    /**
     * Retransmits the offline reply for the current phase without duplicating the reliable handshake.
     *
     * @param ctx connection whose event loop owns the negotiation state
     * @throws IllegalStateException if the initializer has an unknown phase
     */
    @SuppressWarnings("unchecked")
    @Override
    public void sendRequest(final ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();

        final RakNet.Config config = RakNet.config(ctx);
        switch (state) {
            case CR1: {
                if (seenFirst) {
                    final Packet packet = new ConnectionReply1(config.getMagic(),
                            config.getMTU(), config.getServerId());
                    ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                }
                break;
            }
            case CR2: {
                final Packet packet = new ConnectionReply2(config.getMagic(), config.getMTU(),
                        config.getServerId(), (InetSocketAddress) ctx.channel().remoteAddress());
                ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                break;
            }
            case CR3:
                break; // NOOP - ServerHandshake is sent as reliable.
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
    }

    /**
     * Keeps a restart guard after successful negotiation to reject a new session on the same socket.
     *
     * @param ctx pipeline whose handshake handler is being replaced
     */
    @Override
    protected void removeHandler(final ChannelHandlerContext ctx) {
        ctx.channel().pipeline().replace(NAME, NAME, new RestartConnectionHandler());
    }

    /**
     * Binds the first supplied identifier and rejects changes during the same connection attempt.
     *
     * @param ctx connection holding the negotiated client identifier
     * @param clientId identifier carried by a client handshake packet
     * @throws IllegalStateException if a later packet changes the bound identifier
     */
    protected void processClientId(final ChannelHandlerContext ctx, final long clientId) {
        final RakNet.Config config = RakNet.config(ctx);
        if (!clientIdSet) {
            config.setClientId(clientId);
            clientIdSet = true;
        } else if (config.getClientId() != clientId) {
            throw new IllegalStateException("Connection sequence restarted");
        }
    }

    /** Filters stale handshake retries after connection establishment without redelivering them as game data. */
    protected static class RestartConnectionHandler extends ChannelInboundHandlerAdapter {
        /**
         * Ignores a retry from the established peer, closes on a different session, and forwards game data.
         *
         * @param ctx established connection's inbound pipeline
         * @param msg inbound message whose ownership is transferred or consumed here
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (msg instanceof Packet.ClientIdConnection) {
                Packet.ClientIdConnection clientIdConnectionPacket = (Packet.ClientIdConnection) msg;
                final RakNet.Config config = RakNet.config(ctx);
                if (clientIdConnectionPacket.getClientId() == config.getClientId()) {
                    //noinspection RedundantStringFormatCall
                    System.err.println(String.format("Warning: Not restarting RakNet connection for client %s (Possibly caused by high latency)", ctx.channel().remoteAddress()));
                } else {
                    ctx.writeAndFlush(new ConnectionFailed(config.getMagic())).addListener(ChannelFutureListener.CLOSE);
                    ReferenceCountUtil.safeRelease(msg);
                }
            } else if (msg instanceof ConnectionRequest1) {
                final RakNet.Config config = RakNet.config(ctx);
                ctx.writeAndFlush(new ConnectionFailed(config.getMagic())).addListener(ChannelFutureListener.CLOSE);
                ReferenceCountUtil.safeRelease(msg);
            } else if (msg instanceof ConnectionFailed) {
                ReferenceCountUtil.safeRelease(msg);
                ctx.close();
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }

}
