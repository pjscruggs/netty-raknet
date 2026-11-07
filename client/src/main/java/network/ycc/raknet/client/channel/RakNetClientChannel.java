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
import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;
import java.util.List;

/**
 * Runs one client RakNet connection on its UDP socket's event loop.
 * Socket connectivity alone does not make this channel active. The RakNet handshake must also succeed.
 */
public class RakNetClientChannel extends DatagramChannelProxy {
    /**
     * Handshake result shared by the initializer, activity checks and the caller's connect operation.
     */
    protected final ChannelPromise connectPromise;

    /** Optional offline-probe race, configured before registration and owned by the IO event loop. */
    private ClientAddressRace addressRace;

    /**
     * Enables bounded multi-address negotiation without creating multiple application connections.
     * The application must resolve and authorize every endpoint before calling this method.
     * @param addresses ordered resolved candidates, including the endpoint passed to connect first
     * @throws IllegalStateException if registration has already begun
     */
    public void setRemoteAddresses(final List<InetSocketAddress> addresses) {
        if (isRegistered()) {
            throw new IllegalStateException("Configure destinations before registration");
        }
        addressRace = addresses.size() > 1 ? new ClientAddressRace(addresses) : null;
    }

    /**
     * Exposes only the selected peer when a multi-address attempt uses an unconnected UDP socket.
     * @return connected or negotiated peer, or null while offline probes are still racing
     */
    @Override
    public SocketAddress remoteAddress() {
        return addressRace == null ? super.remoteAddress() : addressRace.selected();
    }

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
                final ChannelFuture listenerConnect = addressRace == null
                        ? listener.connect(remoteAddress, localAddress)
                        : listener.bind(localAddress == null ? new InetSocketAddress(0) : localAddress);
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
            if (addressRace != null && msg instanceof ByteBuf) {
                final DatagramPacket datagram = new DatagramPacket((ByteBuf) msg,
                        addressRace.destination(System.nanoTime()));
                if (addressRace.selected() == null) {
                    writeProbe(datagram, promise);
                } else {
                    listener.write(datagram, wrapPromise(promise));
                }
            } else {
                listener.write(msg, wrapPromise(promise));
            }
        }

        /**
         * Allows another candidate to negotiate when a probe fails locally, such as on an IPv4-only host.
         * Only offline probes use this path; established session write failures retain normal propagation.
         * @param datagram owned offline probe transferred to the socket
         * @param promise completed after this attempt is accounted for by the overall handshake
         */
        private void writeProbe(final DatagramPacket datagram, final ChannelPromise promise) {
            final InetSocketAddress destination = datagram.recipient();
            listener.write(datagram).addListener(result -> {
                if (!result.isSuccess() && addressRace.failed(destination)) {
                    connectPromise.tryFailure(result.cause());
                }
                // A failed candidate is not a failed logical connection: the shared handshake
                // deadline and remaining candidates determine the caller-visible result.
                promise.trySuccess();
            });
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
                    if (acceptDatagram(datagram)) {
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
         * Applies handshake selection before the established-peer filter for multi-address clients.
         * @param datagram borrowed envelope being considered for the transport decoder
         * @return whether its payload may enter this connection's pipeline
         */
        private boolean acceptDatagram(final DatagramPacket datagram) {
            return addressRace == null
                    ? datagram.sender() == null || datagram.sender().equals(remoteAddress())
                    : addressRace.accept(datagram, config);
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
