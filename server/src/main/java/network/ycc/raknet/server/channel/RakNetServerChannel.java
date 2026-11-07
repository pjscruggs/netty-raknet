package network.ycc.raknet.server.channel;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.AttributeKey;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.channel.DatagramChannelProxy;
import network.ycc.raknet.packet.NoFreeConnections;
import network.ycc.raknet.packet.Packet;
import network.ycc.raknet.server.RakNetServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Multiplexes incoming UDP peers into child RakNet transports and separately registered application channels.
 * The listening event loop owns the address-to-child map. Per-peer transport and application loops are selected
 * from ServerBootstrap settings, with an optional application-loop override.
 */
public class RakNetServerChannel extends DatagramChannelProxy implements ServerChannel {

    /**
     * Live peer transports indexed by remote address and accessed on the listening channel's event loop.
     */
    protected final Map<SocketAddress, RakNetChildChannel> childMap = new HashMap<>();

    /**
     * Lazily captured bootstrap child settings, available after the server acceptor is installed.
     */
    private ChannelParameters channelParameters;
    /**
     * Optional application-loop override that leaves peer transport registration with the bootstrap child group.
     */
    private EventLoop providedApplicationEventLoop;

    /**
     * Creates an unbound NIO server listener with the default offline admission pipeline.
     */
    public RakNetServerChannel() {
        this(NioDatagramChannel.class);
    }

    /**
     * Creates the listener and retains the socket factory for future per-peer transport sockets.
     *
     * @param ioChannelSupplier factory transferring ownership of a fresh datagram channel on each invocation
     */
    public RakNetServerChannel(final Supplier<? extends DatagramChannel> ioChannelSupplier) {
        super(ioChannelSupplier);
        installServerPipeline(new ServerHandler());
    }

    /**
     * Creates a listener using the selected datagram implementation and its no-argument constructor.
     *
     * @param ioChannelType datagram class compatible with the configured IO event loops
     */
    public RakNetServerChannel(final Class<? extends DatagramChannel> ioChannelType) {
        super(ioChannelType);
        installServerPipeline(new ServerHandler());
    }

    /**
     * Looks up an accepted peer without crossing the listening loop's map-ownership boundary.
     *
     * @param addr remote endpoint used as the peer-map key
     * @return existing child transport, or null if no connection has been entered for that address
     * @throws IllegalStateException if called outside the listening channel's event loop
     */
    public Channel getChildChannel(final SocketAddress addr) {
        if (!eventLoop().inEventLoop()) {
            throw new IllegalStateException("Method must be called from the server eventLoop!");
        }

        return childMap.get(addr);
    }

    /**
     * Selects a separate loop for future application-channel registrations.
     * The override does not move existing channels or change the bootstrap group used for peer transport IO.
     * Set it before accepting connections so registration sees a consistent choice.
     *
     * @param providedApplicationEventLoop optional application loop, or null to use normal ServerBootstrap acceptance
     */
    public void setProvidedApplicationEventLoop(final EventLoop providedApplicationEventLoop) {
        this.providedApplicationEventLoop = providedApplicationEventLoop;
    }

    /**
     * Captures the installed ServerBootstrap acceptor's child settings on first use and reuses that capture.
     * The acceptor must already exist in this server pipeline before the first call.
     *
     * @return child group, handler and option/attribute tables obtained from the bootstrap acceptor
     */
    ChannelParameters getChannelParameters() {
        ChannelParameters parameters = this.channelParameters;
        if (parameters == null) {
            parameters = this.channelParameters = new ChannelParameters();
        }
        return parameters;
    }

    /**
     * Waits for all current application close futures to complete before closing the shared UDP listener.
     * The supplied promise is bridged to the listener close result after the child-close barrier completes.
     *
     * @param promise completion promise for closing the server's listening transport
     */
    @Override
    protected void gracefulClose(final ChannelPromise promise) {
        final PromiseCombiner combined = new PromiseCombiner();
        final ChannelPromise childrenClosed = newPromise();
        childMap.values().forEach(child -> combined.add(child.applicationChannel.close())); // TODO we can technically close the server while keeping all child connections alive
        combined.finish(childrenClosed);
        childrenClosed.addListener(f -> listener.close(wrapPromise(promise)));
    }

    /**
     * Installs the selected peer router and admission stages without invoking subclass hooks during construction.
     *
     * @param handler listener-owned router placed before offline admission processing
     */
    private void installServerPipeline(final ChannelHandler handler) {
        listener.config().setReuseAddress(true);
        pipeline
                .addLast(handler)
                .addLast(RakNetServer.DefaultDatagramInitializer.INSTANCE);
    }



    /**
     * Creates an unregistered peer transport and its application wrapper without adding it to the peer map.
     *
     * @param remoteAddress client endpoint identifying the peer
     * @param localAddress server endpoint on which the initial request arrived
     * @param registerChannel callback responsible for registering the eventual application channel
     * @return new child transport owned by the server's connection-creation path
     */
    protected RakNetChildChannel newChild(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress, final Consumer<Channel> registerChannel) {
        return new RakNetChildChannel(this.ioChannelSupplier, this, remoteAddress, localAddress, registerChannel);
    }

    /**
     * Removes the map entry only if it still refers to the closing child, preserving a replacement connection.
     * Call on the listening event loop, which owns the peer map.
     *
     * @param remoteAddress remote endpoint used when the child was registered
     * @param child exact closing transport that may be removed
     */
    protected void removeChild(final SocketAddress remoteAddress, final RakNetChildChannel child) {
        childMap.remove(remoteAddress, child);
    }

    /**
     * Records a peer transport under its remote endpoint on the listening event loop.
     *
     * @param remoteAddress peer-map key for subsequent datagram routing
     * @param child transport to receive datagrams from that endpoint
     */
    protected void addChild(final SocketAddress remoteAddress, final RakNetChildChannel child) {
        childMap.put(remoteAddress, child);
    }

    /**
     * Exposes the actual listening socket to child setup without transferring its ownership.
     *
     * @return shared datagram listener used for initial connection traffic
     */
    DatagramChannel backingChannel() {
        return this.listener;
    }

    /**
     * Creates peer transports and routes listener datagrams while confining the child map to the listener loop.
     */
    protected class ServerHandler extends ChannelDuplexHandler {
        /**
         * Treats a listener connect operation as child creation for the supplied remote and local internet addresses.
         * An existing peer succeeds without recreation. A new peer receives bootstrap registration and inherited
         * server identity before being entered in the map. The promise reports creation, not handshake completion.
         *
         * @param ctx listening pipeline context used for refusal replies and registration
         * @param remoteAddress client endpoint requiring an internet socket address
         * @param localAddress local internet endpoint that must match the listener's bound address
         * @param promise completion of child creation or lookup
         * @throws IllegalArgumentException if either endpoint is unsuitable or the local address differs from the bind
         */
        @Override
        public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                final SocketAddress localAddress, final ChannelPromise promise) {
            try {
                if (localAddress != null && !RakNetServerChannel.this.localAddress().equals(localAddress)) {
                    throw new IllegalArgumentException(
                            "Bound localAddress does not match provided " + localAddress);
                }
                if (!(remoteAddress instanceof InetSocketAddress)) {
                    throw new IllegalArgumentException(
                            "Provided remote address is not an InetSocketAddress");
                }
                if (!(localAddress instanceof InetSocketAddress)) {
                    throw new IllegalArgumentException(
                            "Provided local address is not an InetSocketAddress");
                }
                final Channel existingChild = getChildChannel(remoteAddress);
                if (childMap.size() > config.getMaxConnections() && existingChild == null) {
                    final Packet packet = new NoFreeConnections(
                            config.getMagic(), config.getServerId());
                    final ByteBuf buf = ctx.alloc().ioBuffer(packet.sizeHint());
                    try {
                        config.getCodec().encode(packet, buf);
                        ctx.writeAndFlush(new DatagramPacket(buf.retain(),
                                (InetSocketAddress) remoteAddress));
                    } finally {
                        ReferenceCountUtil.safeRelease(packet);
                        buf.release();
                    }
                    promise.tryFailure(new IllegalStateException("Too many connections"));
                } else if (existingChild == null) {
                    final RakNetChildChannel child = newChild((InetSocketAddress) remoteAddress, (InetSocketAddress) localAddress, channel -> {
                        EventLoop applicationEventLoop = providedApplicationEventLoop;
                        if (applicationEventLoop != null) {
                            ChannelParameters parameters = getChannelParameters();
                            channel.pipeline().addLast(parameters.childHandler);
                            ChannelUtil.applyChannelParameters(channel, parameters);
                            applicationEventLoop.register(channel);
                        } else {
                            pipeline().fireChannelRead(channel).fireChannelReadComplete(); // register
                        }
                    });
                    child.closeFuture().addListener(v ->
                            eventLoop().execute(() -> removeChild(remoteAddress, child))
                    );
                    child.config().setOption(RakNet.SERVER_ID, config.getServerId());
                    initChildChannel(child);
//                    pipeline().fireChannelRead(child.applicationChannel).fireChannelReadComplete(); //register
                    addChild(remoteAddress, child);
                    promise.trySuccess();
                } else {
                    promise.trySuccess(); //already connected
                }
            } catch (Exception e) {
                promise.tryFailure(e);
                throw e;
            }
        }

        /**
         * Routes a datagram's retained payload to its open, auto-reading child and releases the envelope here.
         * Datagrams from unknown peers continue through offline admission only when they have a real recipient.
         * A null recipient marks an internal replay and prevents another admission pass.
         *
         * @param ctx listener context preceding offline admission
         * @param msg inbound datagram or other message whose ownership transfers into this handler
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (msg instanceof DatagramPacket) {
                final DatagramPacket datagram = (DatagramPacket) msg;
                try {
                    final Channel child = childMap.get(datagram.sender());
                    //a null recipient means this was locally forwarded, dont process again
                    if (child == null && datagram.recipient() != null) {
                        ctx.fireChannelRead(datagram.retain());
                    } else if (child != null && child.isOpen() && child.config().isAutoRead()) {
                        final ByteBuf retained = datagram.content().retain();
                        child.eventLoop().execute(() ->
                            child.pipeline().fireChannelRead(retained).fireChannelReadComplete());
                    }
                } finally {
                    datagram.release();
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }

        /**
         * Notifies each mapped child of listener backpressure before forwarding the listener's own event.
         *
         * @param ctx listening channel context that observed a writability change
         */
        @Override
        public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
            childMap.values().forEach(ch -> ch.pipeline().fireChannelWritabilityChanged());
            ctx.fireChannelWritabilityChanged();
        }

        /**
         * Registers a new peer transport with the child IO group captured from ServerBootstrap.
         * This is distinct from the later registration of its application wrapper.
         *
         * @param child new peer transport awaiting IO registration
         * @return future for registration with the bootstrap child group
         */
        private ChannelFuture initChildChannel(final Channel child) {
            return getChannelParameters().childGroup.register(child);
        }
    }

    /**
     * Captures ServerBootstrap's private acceptor fields needed to reproduce child initialization.
     * Construction requires the acceptor to be installed and its field layout to match the expected Netty API.
     * The captured option and attribute arrays are the acceptor's existing tables, not defensive copies.
     */
    @SuppressWarnings("unchecked")
    class ChannelParameters {
        /**
         * Bootstrap child group used to register each peer's transport pipeline.
         */
        final EventLoopGroup childGroup;
        /**
         * Application initializer captured from the bootstrap and applied when an explicit application loop is used.
         */
        final ChannelHandler childHandler;
        /**
         * Bootstrap option/value table applied to separately initialized application and standalone socket channels.
         */
        final Map.Entry<ChannelOption<?>, Object>[] childOptions;
        /**
         * Bootstrap attribute/value table copied into application channels registered outside the acceptor path.
         */
        final Map.Entry<AttributeKey<?>, Object>[] childAttrs;

        /**
         * Captures bootstrap settings after the server acceptor has been installed.
         *
         * @throws IllegalStateException if the installed Netty bootstrap layout cannot be read
         */
        ChannelParameters() {
            try {
                final Class<? extends ChannelHandler> acceptorClass = (Class<? extends ChannelHandler>) Class.forName("io.netty.bootstrap.ServerBootstrap$ServerBootstrapAcceptor");
                ChannelHandler handler = pipeline().get(acceptorClass);

                {
                    final Field childGroupField = acceptorClass.getDeclaredField("childGroup");
                    childGroupField.setAccessible(true);
                    this.childGroup = (EventLoopGroup) childGroupField.get(handler);
                }

                {
                    final Field childHandlerField = acceptorClass.getDeclaredField("childHandler");
                    childHandlerField.setAccessible(true);
                    this.childHandler = (ChannelHandler) childHandlerField.get(handler);
                }

                {
                    final Field childOptionsField = acceptorClass.getDeclaredField("childOptions");
                    childOptionsField.setAccessible(true);
                    this.childOptions = (Map.Entry<ChannelOption<?>, Object>[]) childOptionsField.get(handler);
                }

                {
                    final Field childAttrsField = acceptorClass.getDeclaredField("childAttrs");
                    childAttrsField.setAccessible(true);
                    this.childAttrs = (Map.Entry<AttributeKey<?>, Object>[]) childAttrsField.get(handler);
                }
            } catch (ReflectiveOperationException exception) {
                throw new IllegalStateException("Cannot read Netty server bootstrap settings", exception);
            }
        }
    }

}

