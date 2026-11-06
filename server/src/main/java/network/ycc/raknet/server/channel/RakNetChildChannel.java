package network.ycc.raknet.server.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;
import network.ycc.raknet.pipeline.FlushTickHandler;
import network.ycc.raknet.server.RakNetServer;
import network.ycc.raknet.server.pipeline.ConnectionInitializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.DatagramPacket;

import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Owns one accepted peer's RakNet framing pipeline and bridges it to a separate application channel.
 * Initial traffic uses the shared server listener. After negotiation, a connected standalone datagram
 * listener can take over the peer's IO while the application retains its own pipeline and event loop.
 */
public class RakNetChildChannel extends AbstractChannel {

    /**
     * Pipeline slot initially occupied by shared-listener writes and later replaced by the standalone socket bridge.
     */
    public static final String WRITE_HANDLER_NAME = "rn-child-channel-write-handler";

    /**
     * Channel capabilities for a peer connection whose disconnect operation closes the connection.
     */
    private static final ChannelMetadata metadata = new ChannelMetadata(false);
    /**
     * Handshake completion used to publish transport activity and close both channel layers on failure.
     */
    protected final ChannelPromise connectPromise;
    /**
     * Allocated peer close promise retained by this class, separate from AbstractChannel closeFuture bookkeeping.
     */
    protected final ChannelPromise closePromise;
    /**
     * Peer-local transport settings initialized from the parent server identity and metrics logger.
     */
    protected final RakNet.Config config;
    /**
     * Client endpoint selected when the server accepts the peer.
     */
    protected final InetSocketAddress remoteAddress;
    /**
     * Local server endpoint on which this peer was accepted and its standalone socket later binds.
     */
    protected final InetSocketAddress localAddress;
    /**
     * Application-facing pipeline paired with this transport and registered through the supplied callback.
     */
    protected final RakNetApplicationChannel applicationChannel;
    /**
     * One-shot guard that prevents repeated attempts to invoke the application-registration callback.
     */
    protected final AtomicBoolean isRegisteringApplicationChannel = new AtomicBoolean(false);
    /**
     * Deferred lifecycle and inbound callbacks awaiting application registration. Payload callbacks hold message ownership until run.
     */
    protected final Queue<Runnable> pendingApplicationChannelOperations = new LinkedList<>();
    /**
     * Server-supplied callback that must arrange application-channel registration after transport negotiation.
     */
    protected final Consumer<Channel> registerChannel;
    /**
     * Standalone per-peer socket created at construction and registered during established-connection setup.
     */
    private final DatagramChannel listener;

    /**
     * Transport lifetime flag cleared by the channel close hook and visible across the paired channel loops.
     */
    protected volatile boolean open = true;

    /**
     * Creates peer-local configuration, an application wrapper and an unregistered standalone socket.
     * The parent continues to receive initial handshake datagrams until standalone-listener setup runs.
     *
     * @param ioChannelSupplier factory transferring ownership of a fresh datagram socket
     * @param parent server channel owning the shared listener and bootstrap settings
     * @param remoteAddress accepted client's internet endpoint
     * @param localAddress local listener endpoint associated with this peer
     * @param registerChannel non-null callback that registers the application wrapper when requested
     */
    public RakNetChildChannel(final Supplier<? extends DatagramChannel> ioChannelSupplier, final Channel parent, final InetSocketAddress remoteAddress, final InetSocketAddress localAddress, final Consumer<Channel> registerChannel) {
        super(parent);
        this.listener = ioChannelSupplier.get();
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.registerChannel = Objects.requireNonNull(registerChannel);
        config = new DefaultConfig(this);
        connectPromise = newPromise();
        closePromise = newPromise();
        config.setMetrics(parent.config().getOption(RakNet.METRICS));
        config.setServerId(parent.config().getOption(RakNet.SERVER_ID));
        this.applicationChannel = new RakNetApplicationChannel(this);
        pipeline().addLast(WRITE_HANDLER_NAME, new WriteHandler());
        addDefaultPipeline();
    }

    /**
     * Installs peer framing and deferred handshake setup, with paired-channel closure when negotiation fails.
     */
    protected void addDefaultPipeline() {
        pipeline().addLast(RakNetServer.DefaultChildInitializer.INSTANCE);
        connectPromise.addListener(x2 -> {
            if (!x2.isSuccess()) {
                RakNetChildChannel.this.applicationChannel.close();
                RakNetChildChannel.this.close();
            }
        });
        pipeline().addLast(new ChannelInitializer<RakNetChildChannel>() {
            /**
             * Replaces the reserved handshake slot and installs the bridge into the application-facing channel.
             *
             * @param ch child channel whose registration triggers initialization
             */
            @Override
            protected void initChannel(final RakNetChildChannel ch) {
                pipeline().replace(ConnectionInitializer.NAME, ConnectionInitializer.NAME,
                        new ConnectionInitializer(connectPromise));
                pipeline().addLast(RakNetApplicationChannel.NAME_SERVER_PARENT_THREADED_READ_HANDLER, new ReadHandler());
            }
        });
    }

    /**
     * Requests application registration once, or drains deferred callbacks after registration has completed.
     * Call from the transport event loop, which owns the pending callback queue. A queued callback failure is
     * reported without preventing subsequent callbacks from running.
     */
    protected void registerApplicationChannelIfNecessary() {
        if (!this.applicationChannel.isRegistered()) {
            if (isRegisteringApplicationChannel.compareAndSet(false, true)) {
                registerChannel.accept(this.applicationChannel);
            }
        } else if (!pendingApplicationChannelOperations.isEmpty()) {
            Runnable r;
            while ((r = pendingApplicationChannelOperations.poll()) != null) {
                try {
                    r.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }

    /**
     * Exposes completion of the peer's RakNet handshake separately from application registration.
     *
     * @return shared negotiation future observed by activity checks and failure cleanup
     */
    public ChannelFuture connectFuture() {
        return connectPromise;
    }

    /**
     * Combines the peer reliability queue's pressure flag with the shared server channel's writability.
     *
     * @return whether neither the peer flag nor the server transport currently blocks writes
     */
    @Override
    public boolean isWritable() {
        final Boolean result = attr(RakNet.WRITABLE).get();
        return (result == null || result) && parent().isWritable();
    }

    /**
     * Reports the parent listener's Netty byte-watermark headroom, excluding this peer's reliability backlog.
     *
     * @return parent-provided number of bytes before the write watermark is exceeded
     */
    @Override
    public long bytesBeforeUnwritable() {
        return parent().bytesBeforeUnwritable();
    }

    /**
     * Reports the parent listener's Netty byte-watermark deficit rather than the peer's unacknowledged data size.
     *
     * @return parent-provided number of bytes that must drain before its write watermark permits writes
     */
    @Override
    public long bytesBeforeWritable() {
        return parent().bytesBeforeWritable();
    }

    /**
     * Returns the accepting server channel that owns the shared UDP listener and peer map.
     *
     * @return existing server parent without transferring resource ownership
     */
    @Override
    public RakNetServerChannel parent() {
        return (RakNetServerChannel) super.parent();
    }

    /**
     * Creates a peer-channel unsafe implementation that rejects initiating another connection.
     *
     * @return internal channel operations for this already accepted peer
     */
    @Override
    protected AbstractUnsafe newUnsafe() {
        return new AbstractUnsafe() {
            /**
             * Rejects outbound connection attempts on a channel already associated with an accepted peer.
             *
             * @param addr1 requested remote address, unused because reconnect is unsupported
             * @param addr2 requested local address, unused because reconnect is unsupported
             * @param pr caller's promise, not completed before this method throws
             * @throws UnsupportedOperationException on every direct connection attempt
             */
            @Override
            public void connect(final SocketAddress addr1, final SocketAddress addr2, final ChannelPromise pr) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Allows the peer framing pipeline to register on any event loop independently of application registration.
     * The standalone datagram socket still requires a compatible loop when it is registered later.
     *
     * @param eventloop proposed loop for the peer transport pipeline
     * @return true for the abstract peer channel itself
     */
    @Override
    protected boolean isCompatible(final EventLoop eventloop) {
        return true;
    }

    /**
     * Returns the local endpoint captured during peer acceptance rather than querying a not-yet-bound socket.
     *
     * @return server endpoint associated with this accepted connection
     */
    @Override
    protected SocketAddress localAddress0() {
        return localAddress;
    }

    /**
     * Returns the immutable peer endpoint captured when the server created this child.
     *
     * @return accepted client's socket address
     */
    @Override
    protected SocketAddress remoteAddress0() {
        return remoteAddress;
    }

    /**
     * Rejects rebinding an accepted child whose local endpoint is selected by the server.
     *
     * @param addr requested replacement local endpoint
     * @throws UnsupportedOperationException on every direct bind attempt
     */
    @Override
    protected void doBind(final SocketAddress addr) {
        throw new UnsupportedOperationException();
    }

    /**
     * Turns a peer disconnect into full channel closure rather than preserving a reconnectable transport.
     */
    @Override
    protected void doDisconnect() {
        close();
    }

    /**
     * Marks the transport closed and releases its socket even if negotiation never installed the listener bridge.
     * Registered sockets normally close through the outbound path before this fallback runs.
     */
    @Override
    protected void doClose() {
        open = false;
        if (listener.isOpen()) {
            if (listener.isRegistered()) {
                listener.close();
            } else {
                listener.unsafe().closeForcibly();
            }
        }
        if (this.applicationChannel.isRegistered()) {
            this.applicationChannel.close();
        }
    }

    /**
     * Leaves inbound reads to the shared or standalone UDP listener rather than issuing a separate peer read.
     */
    @Override
    protected void doBeginRead() {
        // NOOP
    }

    /**
     * Rejects default buffered writes because the transport's outbound handlers forward directly to UDP.
     *
     * @param buffer AbstractChannel buffer that this peer does not use for datagram transmission
     * @throws UnsupportedOperationException whenever the default write hook is invoked
     */
    @Override
    protected void doWrite(final ChannelOutboundBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the peer-local settings shared with its application wrapper.
     *
     * @return live configuration containing negotiated transport state and queue limits
     */
    @Override
    public RakNet.Config config() {
        return config;
    }

    /**
     * Reports this peer's own lifetime independently of whether the shared server listener is still open.
     *
     * @return true until this peer's close hook clears its lifetime flag
     */
    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Requires an open peer, an active parent listener and successful RakNet negotiation.
     *
     * @return whether the accepted peer has an established active transport
     */
    @Override
    public boolean isActive() {
        return isOpen() && parent().isActive() && connectPromise.isSuccess();
    }

    /**
     * Describes this accepted transport as a channel without a reusable disconnect operation.
     *
     * @return shared metadata instance for peer transport channels
     */
    @Override
    public ChannelMetadata metadata() {
        return metadata;
    }

    /**
     * Exposes the paired application pipeline without creating or registering another channel.
     *
     * @return application wrapper belonging to this peer transport
     */
    public RakNetApplicationChannel getApplicationChannel() {
        return this.applicationChannel;
    }

    /**
     * Requests registration and connection of the peer's dedicated UDP socket using captured child options.
     * The socket reuses the accepted local address. Once active, its bridge replaces shared-listener writes.
     * The returned promise is completed by the registration and connect callbacks.
     *
     * @return promise used to observe completion of standalone-listener setup
     */
    protected synchronized ChannelPromise initStandaloneListener() {
        ChannelPromise channelPromise = this.newPromise();
        if (listener.isRegistered()) {
            System.out.println("Already registered?");
            channelPromise.trySuccess();
            return channelPromise;
        }
        ChannelUtil.setChannelOptions(listener, parent().getChannelParameters().childOptions);
        listener.config()
                .setReuseAddress(true)
                .setAutoRead(true)
                .setRecvByteBufAllocator(this.parent().backingChannel().config().getRecvByteBufAllocator());
        listener.pipeline()
                .addLast(new ChannelInboundHandlerAdapter() {
                    /**
                     * Starts standalone socket reads and switches peer writes away from the shared listener.
                     * This one-shot handler removes itself after installing the outbound bridge.
                     *
                     * @param ctx newly active standalone datagram context
                     */
                    @Override
                    public void channelActive(final ChannelHandlerContext ctx) {
                        ctx.fireChannelActive();
                        ctx.read();
                        RakNetChildChannel.this.pipeline().replace(WRITE_HANDLER_NAME, WRITE_HANDLER_NAME, new ListenerOutboundProxy());
                        ctx.pipeline().remove(this);
                    }
                })
                .addLast(WRITE_HANDLER_NAME, new ListenerInboundProxy())
                .addLast(new FlushConsolidationHandler(256, true));
        this.eventLoop().register(this.listener).addListener(future -> {
            if (future.isSuccess()) {
                listener.connect(this.remoteAddress, this.localAddress).addListener(future1 -> {
                    if (future1.isSuccess()) {
                        channelPromise.trySuccess();
                    } else {
                        channelPromise.setFailure(future1.cause());
                        future1.cause().printStackTrace();
                    }
                });
            } else {
                channelPromise.setFailure(future.cause());
                future.cause().printStackTrace();
            }
        });
        return channelPromise;
    }

    /**
     * Creates a listener-owned promise that mirrors success or failure into a peer-pipeline promise.
     *
     * @param in original completion promise belonging to the peer pipeline
     * @return new promise suitable for a standalone socket operation
     */
    protected ChannelPromise wrapPromise(final ChannelPromise in) {
        final ChannelPromise out = listener.newPromise();
        out.addListener(res -> {
            if (res.isSuccess()) {
                in.trySuccess();
            } else {
                in.tryFailure(res.cause());
            }
        });
        return out;
    }

    /**
     * Closes the owned UDP socket before completing transport closure, including sockets not yet registered.
     * This is shared by the initial writer and the established-socket bridge so handshake timing cannot bypass cleanup.
     *
     * @param ctx transport context used to continue closure after socket cleanup
     * @param promise original transport close completion
     */
    private void closeStandaloneListener(final ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (listener.isRegistered()) {
            listener.close().addListener(future -> {
                if (!future.isSuccess()) {
                    ctx.fireExceptionCaught(future.cause());
                }
                ctx.close(promise);
            });
        } else {
            listener.unsafe().closeForcibly();
            ctx.close(promise);
        }
    }

    /**
     * Sends initial peer datagrams through the shared server listener before the standalone socket takes over.
     */
    protected class WriteHandler extends ChannelOutboundHandlerAdapter {
        /**
         * Whether this handler has forwarded a buffer since its last shared-listener flush.
         */
        protected boolean needsFlush;

        /**
         * Releases the peer socket even when shutdown arrives before standalone socket activation.
         *
         * @param ctx initial writer context continuing transport closure
         * @param promise completed after socket and transport cleanup
         */
        @Override
        public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) {
            closeStandaloneListener(ctx, promise);
        }

        /**
         * Wraps encoded buffers in addressed datagrams and transfers them to the shared server listener.
         * The peer promise succeeds on forwarding. Listener write failures are reported through the internal listener.
         * Messages that are not buffers continue through the peer pipeline unchanged.
         *
         * @param ctx outbound peer context used for non-buffer forwarding
         * @param msg encoded buffer or other outbound message whose ownership transfers here
         * @param promise peer write promise acknowledged when a buffer is handed to the parent
         */
        @Override
        public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
            if (msg instanceof ByteBuf) {
                needsFlush = true;
                promise.trySuccess();
                parent().write(new DatagramPacket((ByteBuf) msg, remoteAddress, localAddress))
                        .addListener(RakNet.INTERNAL_WRITE_LISTENER);
            } else {
                ctx.write(msg, promise);
            }
        }

        /**
         * Flushes the shared listener once when this handler has forwarded data since its preceding flush.
         *
         * @param ctx outbound peer context whose flush request is consumed
         */
        @Override
        public void flush(final ChannelHandlerContext ctx) {
            if (needsFlush) {
                needsFlush = false;
                parent().flush();
            }
        }

        /**
         * Consumes explicit peer read requests because the shared listener owns initial datagram reads.
         *
         * @param ctx peer context issuing a read request
         */
        @Override
        public void read(final ChannelHandlerContext ctx) {
            // NOOP
        }
    }

    /**
     * Bridges established peer events into the application pipeline, deferring events while registration is pending.
     */
    protected class ReadHandler extends ChannelInboundHandlerAdapter {

        /**
         * Requests application registration and standalone socket setup before scheduling application activation.
         * Failed socket setup closes the peer without activation. Successful setup defers activation until application registration.
         *
         * @param ctx transport context reporting handshake-driven activity
         * @throws Exception if registration or listener setup throws during this callback
         */
        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            registerApplicationChannelIfNecessary();
            initStandaloneListener().addListener(future -> {
                if (!future.isSuccess()) {
                    ctx.fireExceptionCaught(future.cause());
                    ctx.close();
                    return;
                }
                if (applicationChannel.isRegistered()) {
                    channelActive0();
                } else {
                    pendingApplicationChannelOperations.add(this::channelActive0);
                }
            });
        }

        /**
         * Publishes the active flag and lifecycle event together on the application's registered event loop.
         */
        private void channelActive0() {
            applicationChannel.eventLoop().execute(() -> {
                applicationChannel.setActiveInternal(true);
                applicationChannel.pipeline().fireChannelActive();
            });
        }

        /**
         * Queues or schedules application deactivation depending on whether its registration has completed.
         *
         * @param ctx transport context reporting loss of activity
         * @throws Exception if application registration throws during this callback
         */
        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                channelInactive0();
            } else {
                pendingApplicationChannelOperations.add(this::channelInactive0);
            }
        }

        /**
         * Clears application activity and fires the inactive event on the application's registered event loop.
         */
        private void channelInactive0() {
            applicationChannel.eventLoop().execute(() -> {
                applicationChannel.setActiveInternal(false);
                applicationChannel.pipeline().fireChannelInactive();
            });
        }

        /**
         * Transfers an inbound message to the application now or holds its ownership in a deferred callback.
         * No additional reference is retained by this bridge. The eventual application pipeline owns release.
         *
         * @param ctx peer transport context delivering the message
         * @param msg inbound message reference transferred to application delivery
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                applicationChannel.pipeline().fireChannelRead(msg);
            } else {
                pendingApplicationChannelOperations.add(() -> applicationChannel.pipeline().fireChannelRead(msg));
            }
        }

        /**
         * Relays a read-batch boundary only when the application is registered.
         * Unlike payload callbacks, read-complete events are not queued during registration.
         *
         * @param ctx peer transport context reporting completion of a read batch
         */
        @Override
        public void channelReadComplete(final ChannelHandlerContext ctx) {
            if (!applicationChannel.isRegistered()) {
                return;
            }
            applicationChannel.pipeline().fireChannelReadComplete();
        }

        /**
         * Keeps transport flush-control events local and forwards other user events to the application.
         * Events arriving before application registration are held by a deferred delivery callback.
         *
         * @param ctx peer transport context delivering the event
         * @param evt transport control signal or application-visible user event
         */
        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
            if (applicationChannel.isRegistered()) {
                registerApplicationChannelIfNecessary();
            }
            if (evt == FlushTickHandler.FLUSH_CHECK_SIGNAL || evt instanceof FlushTickHandler.MissedFlushes) {
                return;
            }
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                applicationChannel.pipeline().fireUserEventTriggered(evt);
            } else {
                pendingApplicationChannelOperations.add(() -> applicationChannel.pipeline().fireUserEventTriggered(evt));
            }
        }

        /**
         * Relays peer pressure changes immediately or after application registration so handlers can resume writes.
         *
         * @param ctx peer transport context whose writability changed
         */
        @Override
        public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                applicationChannel.pipeline().fireChannelWritabilityChanged();
            } else {
                pendingApplicationChannelOperations.add(() -> applicationChannel.pipeline().fireChannelWritabilityChanged());
            }
        }

        /**
         * Delivers transport failures to the application pipeline, deferring them until registration if necessary.
         *
         * @param ctx peer context reporting the failure
         * @param cause exception to forward to application handlers
         */
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                applicationChannel.pipeline().fireExceptionCaught(cause);
            } else {
                pendingApplicationChannelOperations.add(() -> applicationChannel.pipeline().fireExceptionCaught(cause));
            }
        }
    }

    /**
     * Forwards established peer writes and flushes to its connected standalone datagram socket.
     */
    protected class ListenerOutboundProxy implements ChannelOutboundHandler {

        /**
         * Checks the expected event-loop ownership when the standalone outbound bridge is installed.
         *
         * @param ctx peer pipeline context that installs this listener-bound handler
         */
        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
            assert listener.eventLoop().inEventLoop();
        }

        /**
         * Rejects rebinding an established peer by failing its supplied promise.
         *
         * @param ctx peer context issuing the bind request
         * @param localAddress requested replacement endpoint
         * @param promise completed with UnsupportedOperationException
         */
        @Override
        public void bind(final ChannelHandlerContext ctx, final SocketAddress localAddress,
                         final ChannelPromise promise) {
//            listener.bind(localAddress, wrapPromise(promise));
            promise.setFailure(new UnsupportedOperationException());
        }

        /**
         * Rejects reconnecting a peer whose standalone socket already belongs to an accepted connection.
         *
         * @param ctx peer context issuing the connect request
         * @param remoteAddress requested replacement peer
         * @param localAddress requested replacement local endpoint
         * @param promise completed with UnsupportedOperationException
         */
        @Override
        public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                            final SocketAddress localAddress, final ChannelPromise promise) {
//            listener.connect(remoteAddress, localAddress, wrapPromise(promise));
            promise.setFailure(new UnsupportedOperationException());
        }

        /**
         * Performs no separate cleanup on removal because the bridge's close operation owns listener shutdown.
         *
         * @param ctx peer context from which the outbound bridge was removed
         */
        @Override
        public void handlerRemoved(final ChannelHandlerContext ctx) {
            // NOOP
        }

        /**
         * Rejects socket-only disconnection so callers must close the accepted peer connection instead.
         *
         * @param ctx peer context issuing the disconnect request
         * @param promise completed with UnsupportedOperationException
         */
        @Override
        public void disconnect(final ChannelHandlerContext ctx, final ChannelPromise promise) {
//            listener.disconnect(wrapPromise(promise));
            promise.setFailure(new UnsupportedOperationException());
        }

        /**
         * Closes a registered standalone listener before continuing peer-pipeline closure.
         * A listener close failure is reported, then the original peer close request still proceeds.
         *
         * @param ctx peer outbound context used to continue the close operation
         * @param promise caller's peer-close completion promise
         */
        @Override
        public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) {
            closeStandaloneListener(ctx, promise);
        }

        /**
         * Passes peer deregistration to the preceding outbound handlers without independently deregistering the socket.
         *
         * @param ctx peer outbound context
         * @param promise completion promise forwarded with the deregistration request
         */
        @Override
        public void deregister(final ChannelHandlerContext ctx, final ChannelPromise promise) {
            ctx.deregister(promise);
        }

        /**
         * Ignores explicit reads because the standalone listener is configured to drive its own incoming IO.
         *
         * @param ctx peer context issuing an explicit read request
         */
        @Override
        public void read(final ChannelHandlerContext ctx) {
            // NOOP
        }

        /**
         * Transfers an encoded outbound message to the standalone socket and bridges the listener write result.
         *
         * @param ctx peer outbound context supplied by Netty
         * @param msg outbound message reference transferred to the listener
         * @param promise completed with the standalone socket's write outcome
         */
        @Override
        public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
            listener.write(msg, wrapPromise(promise)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        }

        /**
         * Flushes queued standalone socket writes without creating a second transport flush queue.
         *
         * @param ctx peer context whose flush request is forwarded
         */
        @Override
        public void flush(final ChannelHandlerContext ctx) {
            listener.flush();
        }

        /**
         * Ignores a no-route notification and forwards other failures through the peer exception pipeline.
         *
         * @param ctx peer outbound bridge context
         * @param cause socket or pipeline failure reported to this handler
         */
        @SuppressWarnings("deprecation")
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (cause instanceof NoRouteToHostException) {
                return;
            }
            ctx.fireExceptionCaught(cause);
        }

    }

    /**
     * Transfers standalone socket payloads into peer decoding while keeping raw socket lifecycle events local.
     */
    protected class ListenerInboundProxy implements ChannelInboundHandler {

        /**
         * Consumes raw socket registration because peer and application registration are managed separately.
         *
         * @param ctx standalone socket context reporting this event
         */
        @Override
        public void channelRegistered(final ChannelHandlerContext ctx) {
        }

        /**
         * Consumes raw socket deregistration rather than publishing it as application-channel deregistration.
         *
         * @param ctx standalone socket context reporting this event
         */
        @Override
        public void channelUnregistered(final ChannelHandlerContext ctx) {
        }

        /**
         * Checks that installation occurs on the standalone listener event loop.
         *
         * @param ctx standalone socket context reporting this event
         */
        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
            assert listener.eventLoop().inEventLoop();
        }

        /**
         * Consumes socket activity because RakNet handshake completion determines peer activity.
         *
         * @param ctx standalone socket context reporting this event
         */
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            // NOOP - active status managed by connection sequence
        }

        /**
         * Consumes raw socket inactivity without independently issuing an application lifecycle event.
         *
         * @param ctx standalone socket context reporting this event
         */
        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
        }

        /**
         * Performs no separate resource action when the standalone inbound bridge is removed.
         *
         * @param ctx standalone socket context reporting this event
         */
        @Override
        public void handlerRemoved(final ChannelHandlerContext ctx) {
            // NOOP
        }

        /**
         * Retains and forwards datagram payloads only while the peer is active, then releases each envelope.
         * Non-datagram messages transfer directly into the peer pipeline with their existing ownership.
         *
         * @param ctx standalone listener context receiving the message
         * @param msg datagram envelope or other inbound message owned by this callback
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (msg instanceof DatagramPacket) {
                DatagramPacket datagram = (DatagramPacket) msg;
                try {
                    if (isActive()) {
                        final ByteBuf retained = datagram.content().retain();
                        pipeline().fireChannelRead(retained);
                    }
                } finally {
                    datagram.release();
                }
            } else {
                pipeline().fireChannelRead(msg);
            }
        }

        /**
         * Marks the end of a standalone socket read batch in the peer decoding pipeline.
         *
         * @param ctx standalone listener context reporting read completion
         */
        @Override
        public void channelReadComplete(final ChannelHandlerContext ctx) {
            pipeline().fireChannelReadComplete();
        }

        /**
         * Consumes socket-local user events instead of exposing them to the RakNet peer pipeline.
         *
         * @param ctx standalone socket context receiving the event
         * @param evt socket-local event consumed by this bridge
         */
        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
            // NOOP
        }

        /**
         * Notifies the peer pipeline when standalone socket backpressure changes.
         *
         * @param ctx standalone socket context whose writability changed
         */
        @Override
        public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
            pipeline().fireChannelWritabilityChanged();
        }

        /**
         * Ignores closed-socket notifications and forwards other listener failures to the peer pipeline.
         *
         * @param ctx standalone socket context reporting a failure
         * @param cause socket failure to classify or forward
         */
        @SuppressWarnings("deprecation")
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (cause instanceof ClosedChannelException) {
                return;
            }
            pipeline().fireExceptionCaught(cause);
        }

    }

}
