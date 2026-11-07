package network.ycc.raknet.client.channel;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.channel.DatagramChannelProxy;
import network.ycc.raknet.client.RakNetClient;
import network.ycc.raknet.client.pipeline.ConnectionInitializer;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;

/**
 * Runs one client RakNet connection on its UDP socket's event loop.
 * Socket connectivity alone does not make this channel active. The RakNet handshake must also succeed.
 */
public class RakNetClientChannel extends DatagramChannelProxy {
    /**
     * Handshake result shared by the initializer, activity checks and the caller's connect operation.
     */
    protected final ChannelPromise connectPromise;

    /**
     * Creates an unconnected client using a NIO datagram socket and the default RakNet pipeline.
     */
    public RakNetClientChannel() {
        this(NioDatagramChannel.class);
    }

    /**
     * Creates a client around a fresh socket from the supplied factory and installs connection handlers.
     *
     * @param ioChannelSupplier factory that transfers ownership of an unregistered datagram channel
     */
    public RakNetClientChannel(final Supplier<? extends DatagramChannel> ioChannelSupplier) {
        super(ioChannelSupplier);
        connectPromise = pipeline.newPromise();
        installClientPipeline(new ClientHandler());
    }

    /**
     * Creates a client socket through the supplied channel class's no-argument constructor.
     *
     * @param ioChannelType datagram implementation compatible with the event loop used at registration
     */
    public RakNetClientChannel(final Class<? extends DatagramChannel> ioChannelType) {
        super(ioChannelType);
        connectPromise = pipeline.newPromise();
        installClientPipeline(new ClientHandler());
    }

    /**
     * Exposes completion of RakNet negotiation independently of the socket's UDP connect operation.
     *
     * @return shared handshake future, successful only after the server handshake is accepted
     */
    public ChannelFuture connectFuture() {
        return connectPromise;
    }

    /**
     * Requires both an active underlying socket and a successfully completed RakNet handshake.
     *
     * @return whether established application traffic can use this transport
     */
    @Override
    public boolean isActive() {
        return super.isActive() && connectPromise.isSuccess();
    }

    /**
     * Combines the reliability handler's queue-pressure flag with the UDP socket's writability.
     * An absent RakNet flag imposes no additional restriction before reliability setup initializes it.
     *
     * @return whether neither transport-level nor socket-level backpressure currently blocks writes
     */
    @Override
    public boolean isWritable() {
        final Boolean result = attr(RakNet.WRITABLE).get();
        return (result == null || result) && super.isWritable();
    }

    /**
     * Installs the selected socket router and protocol stages without invoking subclass hooks during construction.
     *
     * @param handler connection-local router placed before the standard RakNet stages
     */
    private void installClientPipeline(final ChannelHandler handler) {
        pipeline
                .addLast(handler)
                .addLast(RakNetClient.DefaultClientInitializer.INSTANCE);
        connectPromise.addListener(res -> {
            if (!res.isSuccess()) {
                RakNetClientChannel.this.close();
            }
        });
    }



    /**
     * Starts negotiation after UDP connect and filters incoming datagrams to the selected remote endpoint.
     */
    protected class ClientHandler extends ChannelDuplexHandler {
        /**
         * Starts UDP connection and replaces the handshake placeholder after socket connection succeeds.
         * The supplied promise combines socket connection and RakNet negotiation. Validation failures fail it directly.
         *
         * @param ctx client pipeline context supplied by Netty
         * @param remoteAddress required internet socket address of the RakNet server
         * @param localAddress optional local bind address passed to the datagram channel
         * @param promise caller's completion promise for the combined connection attempt
         */
        @Override
        public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                final SocketAddress localAddress, final ChannelPromise promise) {
            try {
                if (!(remoteAddress instanceof InetSocketAddress)) {
                    throw new IllegalArgumentException(
                            "Provided remote address is not an InetSocketAddress");
                }
                if (listener.isActive()) {
                    throw new IllegalStateException("Channel connection already started");
                }
                final ChannelFuture listenerConnect = listener.connect(remoteAddress, localAddress);
                listenerConnect.addListener(udpConnectResult -> {
                    if (udpConnectResult.isSuccess()) {
                        //start connection process
                        pipeline().replace(ConnectionInitializer.NAME, ConnectionInitializer.NAME,
                                new ConnectionInitializer(connectPromise));
                    } else {
                        connectPromise.tryFailure(udpConnectResult.cause());
                    }
                });
                final PromiseCombiner combiner = new PromiseCombiner();
                combiner.add(listenerConnect);
                combiner.add((ChannelFuture) connectPromise);
                combiner.finish(promise);
            } catch (Exception t) {
                promise.tryFailure(t);
            }
        }

        /**
         * Transfers an encoded message to the UDP socket and bridges its write result to the caller's promise.
         *
         * @param ctx client pipeline context supplied by Netty
         * @param msg outbound message whose ownership transfers to the listener
         * @param promise completed with the listener's write outcome
         */
        @Override
        public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
            listener.write(msg, wrapPromise(promise));
        }

        /**
         * Forwards a retained payload only when its datagram sender matches the connected peer or is unspecified.
         * Every datagram envelope is released here, including packets from other senders. Non-datagram messages
         * are forwarded unchanged with their existing ownership.
         *
         * @param ctx next stage of the client decoding pipeline
         * @param msg inbound envelope or other message owned by this handler
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (msg instanceof DatagramPacket) {
                try {
                    final DatagramPacket datagram = (DatagramPacket) msg;
                    if (datagram.sender() == null || datagram.sender().equals(remoteAddress())) {
                        ctx.fireChannelRead(datagram.content().retain());
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }

        /**
         * Ignores explicit reads at this layer because the underlying datagram channel drives socket reads.
         *
         * @param ctx outbound client context whose read request is consumed
         */
        @Override
        public void read(final ChannelHandlerContext ctx) {
            // NOOP
        }
    }
}
