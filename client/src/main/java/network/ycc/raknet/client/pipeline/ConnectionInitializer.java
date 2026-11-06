package network.ycc.raknet.client.pipeline;

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

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;

/**
 * Negotiates the client MTU and server identity, then completes the reliable connection handshake.
 */
public class ConnectionInitializer extends AbstractConnectionInitializer {

    /**
     * Number of phase-one probes already sent, used to reduce subsequent probe sizes in 16-byte steps.
     */
    private int cr1Retries;

    /**
     * Associates this negotiation with the client channel's connection-completion promise.
     *
     * @param connectPromise completed when the handshake succeeds or its failure becomes terminal
     */
    public ConnectionInitializer(final ChannelPromise connectPromise) {
        super(connectPromise);
    }

    /**
     * Advances the client handshake or completes a remote rejection immediately in every phase.
     *
     * @param ctx client connection closed when the server rejects its handshake
     * @param msg decoded server handshake response
     * @throws IllegalStateException if this initializer has an unsupported internal state
     */
    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final Packet msg) {
        final RakNet.Config config = RakNet.config(ctx);
        if (msg instanceof ConnectionFailed) {
            fail(new ChannelException("RakNet connection failed"));
            ctx.close();
            return;
        }
        switch (state) {
            case CR1: {
                if (msg instanceof ConnectionReply1) {
                    final ConnectionReply1 cr1 = (ConnectionReply1) msg;
                    cr1.getMagic().verify(config.getMagic());
                    config.setMTU(cr1.getMtu());
                    config.setServerId(cr1.getServerId());
                    state = State.CR2;
                } else if (msg instanceof InvalidVersion) {
                    fail(new InvalidVersion.InvalidVersionException());
                }
                break;
            }
            case CR2: {
                if (msg instanceof ConnectionReply2) {
                    final ConnectionReply2 cr2 = (ConnectionReply2) msg;
                    cr2.getMagic().verify(config.getMagic());
                    config.setMTU(cr2.getMtu());
                    config.setServerId(cr2.getServerId());
                    state = State.CR3;
                    final Packet packet = new ConnectionRequest(config.getClientId());
                    ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                }
                break;
            }
            case CR3: {
                if (msg instanceof ServerHandshake) {
                    final Packet packet = new ClientHandshake(
                            ((ServerHandshake) msg).getTimestamp(),
                            (InetSocketAddress) ctx.channel().remoteAddress(),
                            ((ServerHandshake) msg).getnExtraAddresses());
                    ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                    startPing(ctx);
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
     * Sends the current offline probe, reducing phase-one probe sizes down to a 130-byte floor.
     * Phase three needs no periodic resend here because its handshake packet uses reliable delivery.
     *
     * @param ctx client transport pipeline used to send the probe
     * @throws IllegalStateException if the initializer has an unknown handshake phase
     */
    @Override
    public void sendRequest(final ChannelHandlerContext ctx) {
        final RakNet.Config config = RakNet.config(ctx);
        switch (state) {
            case CR1: {
                final Packet packet = new ConnectionRequest1(config.getMagic(),
                        config.getProtocolVersion(), Math.max(config.getMTU() - (cr1Retries * 16), 130));
                cr1Retries += 1;
                ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                break;
            }
            case CR2: {
                final Packet packet = new ConnectionRequest2(config.getMagic(), config.getMTU(),
                        config.getClientId(), (InetSocketAddress) ctx.channel().remoteAddress());
                ctx.writeAndFlush(packet).addListener(RakNet.INTERNAL_WRITE_LISTENER);
                break;
            }
            case CR3:
                break; // NOOP - ClientHandshake is sent as reliable.
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
    }

    /**
     * Removes this handshake handler after success so subsequent packets reach established handlers.
     *
     * @param ctx client pipeline containing this initializer
     */
    @Override
    protected void removeHandler(final ChannelHandlerContext ctx) {
        ctx.channel().pipeline().remove(this);
    }

}
