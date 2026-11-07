package network.ycc.raknet.client.channel;

import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.concurrent.PromiseCombiner;
import network.ycc.raknet.RakNet;

import java.net.SocketAddress;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * Exposes an application pipeline separately from the parent RakNet transport pipeline.
 * Writes and configuration are delegated to the parent. Application handlers may use a different event loop,
 * while the parent retains responsibility for datagram IO, reliability and connection negotiation.
 */
public class RakNetClientThreadedChannel extends AbstractChannel {

    /**
     * Parent-pipeline name of the bridge that delivers established transport events to the application.
     */
    public static final String NAME_CLIENT_PARENT_THREADED_READ_HANDLER = "rn-client-parent-threaded-read-handler";

    /**
     * Optional caller-selected IO event loop, configured before the parent is registered.
     */
    private EventLoop providedParentEventLoop;
    /**
     * Application registration loop remembered as the fallback parent loop when no IO loop was supplied.
     */
    private EventLoop pendingEventLoop;

    /**
     * Application-visible activity state, published when parent lifecycle events reach the application loop.
     */
    private volatile boolean active;

    /** Application-loop-owned completion waiting for both transport success and application activity. */
    private ChannelPromise pendingConnect;
    /** Whether the parent connection result has succeeded for the current application attempt. */
    private boolean parentConnected;

    /**
     * Creates an unregistered application channel backed by a default NIO RakNet client transport.
     */
    public RakNetClientThreadedChannel() {
        super(new RakNetClientChannel());
        setupDefaultPipeline();
    }

    /**
     * Creates an application wrapper around a client socket instantiated from the selected class.
     *
     * @param ioChannelType datagram implementation to register on a compatible parent event loop
     */
    public RakNetClientThreadedChannel(final Class<? extends DatagramChannel> ioChannelType) {
        super(new RakNetClientChannel(ioChannelType));
        setupDefaultPipeline();
    }

    /**
     * Creates an application wrapper around a fresh client transport from the supplied socket factory.
     *
     * @param ioChannelSupplier factory transferring ownership of a new unregistered datagram channel
     */
    public RakNetClientThreadedChannel(final Supplier<? extends DatagramChannel> ioChannelSupplier) {
        super(new RakNetClientChannel(ioChannelSupplier));
        setupDefaultPipeline();
    }

    /**
     * Retains the older name for selecting the parent IO loop before connection begins.
     *
     * @param providedEventLoop loop compatible with the selected datagram implementation
     * @deprecated use setProvidedParentEventLoop to distinguish IO registration from application registration
     */
    @Deprecated
    public void setProvidedEventLoop(final EventLoop providedEventLoop) {
        this.setProvidedParentEventLoop(providedEventLoop);
    }

    /**
     * Selects the parent's IO event loop before its first bind or connect operation.
     * Supply a compatible IO loop when the application loop cannot register the chosen datagram implementation.
     * A null value uses the loop remembered during application registration.
     *
     * @param providedParentEventLoop optional event loop that will own the client transport
     */
    public void setProvidedParentEventLoop(final EventLoop providedParentEventLoop) {
        this.providedParentEventLoop = providedParentEventLoop;
    }

    /**
     * Adds a deferred initializer that installs the parent-to-application event bridge when the parent registers.
     */
    private void setupDefaultPipeline() {
        super.parent().pipeline().addLast(new ChannelInitializer<Channel>() {
            /**
             * Installs the event bridge after parent registration, before established transport events reach the tail.
             *
             * @param channel parent RakNet client pipeline being initialized
             */
            @Override
            protected void initChannel(final Channel channel) {
                channel.pipeline().addLast(NAME_CLIENT_PARENT_THREADED_READ_HANDLER, new ParentReadHandler());
            }
        });
    }

    /**
     * Combines application-level queue pressure with the parent transport's current writability.
     *
     * @return whether both the local RakNet flag and parent permit another write
     */
    @Override
    public boolean isWritable() {
        final Boolean result = attr(RakNet.WRITABLE).get();
        return (result == null || result) && parent().isWritable();
    }

    /**
     * Reports the parent's Netty byte-watermark estimate without including this channel's separate RakNet flag.
     *
     * @return parent-provided bytes that can be queued before its write watermark is exceeded
     */
    @Override
    public long bytesBeforeUnwritable() {
        return parent().bytesBeforeUnwritable();
    }

    /**
     * Reports the parent's Netty byte-watermark deficit rather than a count of unacknowledged RakNet data.
     *
     * @return parent-provided number of queued bytes that must drain before its watermark allows writes
     */
    @Override
    public long bytesBeforeWritable() {
        return parent().bytesBeforeWritable();
    }

    /**
     * Exposes the transport that owns this application's connection and packet-processing pipeline.
     *
     * @return the existing parent transport channel, without transferring ownership
     */
    @Override
    public RakNetClientChannel parent() {
        return (RakNetClientChannel) super.parent();
    }

    /**
     * Creates the channel-internal bridge that redirects writes and lifecycle operations to the parent.
     *
     * @return connection-local unsafe implementation used by AbstractChannel
     */
    @Override
    protected AbstractUnsafe newUnsafe() {
        return new RakNetClientThreadedChannelUnsafe();
    }

    /**
     * Returns the intercepting unsafe facade rather than exposing AbstractChannel's default outbound path.
     *
     * @return facade that sends writes through the parent and coordinates parent/application closure
     */
    @Override
    public Unsafe unsafe() {
        return ((RakNetClientThreadedChannelUnsafe) super.unsafe()).wrapped;
    }

    /**
     * Accepts any application loop and remembers it as the default for later parent registration.
     * Compatibility of the actual datagram implementation must be provided by the selected parent loop.
     *
     * @param eventloop application loop proposed by AbstractChannel registration
     * @return true because this wrapper has no direct socket registration requirements
     */
    @Override
    protected boolean isCompatible(final EventLoop eventloop) {
        this.pendingEventLoop = eventloop;
        return true;
    }

    /**
     * Reads the bound endpoint from the transport instead of keeping an independent application address.
     *
     * @return parent's local address, or null before it has one
     */
    @Override
    protected SocketAddress localAddress0() {
        return parent().localAddress();
    }

    /**
     * Reads the connected peer from the transport instead of maintaining a second remote endpoint.
     *
     * @return parent's remote address, or null before the transport has selected one
     */
    @Override
    protected SocketAddress remoteAddress0() {
        return parent().remoteAddress();
    }

    /**
     * Rejects AbstractChannel's direct bind hook because this application channel owns no socket.
     *
     * @param addr requested local address, unused because direct binding is unsupported
     * @throws UnsupportedOperationException whenever this direct bind hook is invoked
     */
    @Override
    protected void doBind(final SocketAddress addr) {
        throw new UnsupportedOperationException();
    }

    /**
     * Marks application activity false and requests closure of the parent RakNet connection.
     */
    @Override
    protected void doDisconnect() {
//        close();
        this.active = false;
        failPendingConnect(new ClosedChannelException());
        parent().close();
    }

    /**
     * Marks application activity false and initiates closure of the transport that owns its resources.
     */
    @Override
    protected void doClose() {
//        close();
        this.active = false;
        failPendingConnect(new ClosedChannelException());
        parent().close();
    }

    /**
     * Performs no socket read because inbound delivery is driven by the parent pipeline.
     */
    @Override
    protected void doBeginRead() {
        // NOOP
    }

    /**
     * Rejects the default buffered-write path because the unsafe facade transfers messages directly to the parent.
     *
     * @param buffer outbound buffer that this channel does not use for transmission
     * @throws UnsupportedOperationException whenever the default buffered-write hook is invoked
     */
    @Override
    protected void doWrite(final ChannelOutboundBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    /**
     * Shares the parent's transport configuration so both pipelines observe the same negotiated settings.
     *
     * @return the parent's live configuration object, not a copy
     */
    @Override
    public RakNet.Config config() {
        return parent().config();
    }

    /**
     * Uses the transport's open state because this application wrapper owns no independent socket.
     *
     * @return whether the parent connection is still open
     */
    @Override
    public boolean isOpen() {
        return parent().isOpen();
    }

    /**
     * Reports the activity state delivered to this application pipeline by the parent lifecycle bridge.
     *
     * @return most recently published application activity flag
     */
    @Override
    public boolean isActive() {
        return this.active;
    }

    /**
     * Exposes the parent's transport metadata rather than advertising separate application socket capabilities.
     *
     * @return the parent channel's metadata
     */
    @Override
    public ChannelMetadata metadata() {
        return parent().metadata();
    }

    /**
     * Coordinates application-channel operations with the parent while bypassing a second outbound buffer.
     */
    protected final class RakNetClientThreadedChannelUnsafe extends AbstractUnsafe {

        // intercept calls
        /**
         * Intercepting facade that retains application promises while forwarding actual IO to the parent.
         */
        final Unsafe wrapped = new Unsafe() {
            /**
             * Uses this application's allocator handle for Netty bookkeeping without performing socket reads here.
             *
             * @return receive-allocation handle maintained by the enclosing AbstractUnsafe
             */
            @SuppressWarnings("deprecation")
            @Override
            public RecvByteBufAllocator.Handle recvBufAllocHandle() {
                return RakNetClientThreadedChannelUnsafe.this.recvBufAllocHandle();
            }

            /**
             * Resolves the application address through the enclosing unsafe implementation and parent transport.
             *
             * @return currently bound local endpoint, or null before binding
             */
            @Override
            public SocketAddress localAddress() {
                return RakNetClientThreadedChannelUnsafe.this.localAddress();
            }

            /**
             * Resolves the application peer through the enclosing unsafe implementation and parent transport.
             *
             * @return selected remote endpoint, or null before connection
             */
            @Override
            public SocketAddress remoteAddress() {
                return RakNetClientThreadedChannelUnsafe.this.remoteAddress();
            }

            /**
             * Registers the application pipeline with its selected event loop without registering a second socket.
             *
             * @param eventLoop loop that will execute this application's handlers
             * @param promise completed by AbstractUnsafe when registration succeeds or fails
             */
            @Override
            public void register(final EventLoop eventLoop, final ChannelPromise promise) {
                RakNetClientThreadedChannelUnsafe.this.register(eventLoop, promise);
            }

            /**
             * Registers the parent if needed, asks it to bind the socket, and mirrors the bind result.
             *
             * @param localAddress local endpoint passed to the parent transport
             * @param promise application promise completed with the parent's bind outcome
             */
            @Override
            public void bind(final SocketAddress localAddress, final ChannelPromise promise) {
                registerParent();
                final ChannelFuture future = parent().bind(localAddress);
                future.addListener(RakNet.INTERNAL_WRITE_LISTENER);
                future.addListener(future1 -> {
                    if (future1.isSuccess()) {
                        promise.trySuccess();
                    } else {
                        promise.tryFailure(future1.cause());
                    }
                });
            }

            /**
             * Delegates connection setup to the enclosing unsafe implementation, which registers and connects the parent.
             *
             * @param remoteAddress target server endpoint
             * @param localAddress optional local bind endpoint
             * @param promise completed with the parent connection result
             */
            @Override
            public void connect(final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
                RakNetClientThreadedChannelUnsafe.this.connect(remoteAddress, localAddress, promise);
            }

            /**
             * Runs AbstractUnsafe's disconnect lifecycle, which clears activity and requests parent closure.
             *
             * @param promise completion of the application's disconnect operation
             */
            @Override
            public void disconnect(final ChannelPromise promise) {
                RakNetClientThreadedChannelUnsafe.this.disconnect(promise);
            }

            /**
             * Closes the parent first, then queues application closure after the parent's deregistration work.
             * The supplied promise combines the parent result and the application close result.
             *
             * @param promise completed after both sides of the connection-close sequence finish
             */
            @Override
            public void close(final ChannelPromise promise) {
                EventLoop parentEventLoop = parent().eventLoop(); // to prevent deregister race
                parent().close().addListener(future -> {
                    PromiseCombiner combiner = new PromiseCombiner();
                    ChannelPromise newPromise = promise.channel().newPromise();
                    parentEventLoop.execute(() -> { // wait for parent to finish deregistering
                        promise.channel().eventLoop().execute(() -> {
                            RakNetClientThreadedChannelUnsafe.this.close(newPromise);
                        });
                    });
                    combiner.add((ChannelFuture) newPromise);
                    combiner.add(future);
                    combiner.finish(promise);
                });
            }

            /**
             * Invokes AbstractUnsafe's emergency close hook, which marks this application inactive and closes its parent.
             */
            @Override
            public void closeForcibly() {
                RakNetClientThreadedChannelUnsafe.this.closeForcibly();
            }

            /**
             * Deregisters this application pipeline through AbstractUnsafe without transferring socket ownership.
             *
             * @param promise completed when application deregistration finishes
             */
            @Override
            public void deregister(final ChannelPromise promise) {
                RakNetClientThreadedChannelUnsafe.this.deregister(promise);
            }

            /**
             * Invokes the application read hook, which is a no-op because the parent supplies inbound events.
             */
            @Override
            public void beginRead() {
                RakNetClientThreadedChannelUnsafe.this.beginRead();
            }

            /**
             * Transfers message ownership directly to the parent pipeline and mirrors its write result.
             * No duplicate or retained copy is made at this boundary.
             *
             * @param msg outbound application message transferred to the parent
             * @param promise completed with the parent's success or failure
             */
            @Override
            public void write(final Object msg, final ChannelPromise promise) {
                // we don't use ChannelOutboundBuffer
                final ChannelFuture future = parent().write(msg);
                future.addListener(RakNet.INTERNAL_WRITE_LISTENER);
                future.addListener(future1 -> {
                    if (future1.isSuccess()) {
                        promise.trySuccess();
                    } else {
                        promise.tryFailure(future1.cause());
                    }
                });
            }

            /**
             * Flushes pending parent writes without maintaining another application-level outbound buffer.
             */
            @Override
            public void flush() {
                parent().flush();
            }

            /**
             * Returns AbstractUnsafe's reusable promise for operations whose completion the caller does not observe.
             *
             * @return this application's void promise
             */
            @Override
            public ChannelPromise voidPromise() {
                return RakNetClientThreadedChannelUnsafe.this.voidPromise();
            }

            /**
             * Exposes AbstractUnsafe's bookkeeping buffer, although ordinary writes bypass it through the facade.
             *
             * @return the underlying outbound buffer, which may be null after closure
             */
            @Override
            public ChannelOutboundBuffer outboundBuffer() {
                return RakNetClientThreadedChannelUnsafe.this.outboundBuffer();
            }
        };

        /**
         * Registers the parent and waits for both negotiated transport and application activation before success.
         *
         * @param remoteAddress target RakNet server endpoint
         * @param localAddress optional local bind endpoint
         * @param promise application promise receiving the parent connection's outcome
         */
        @Override
        public void connect(final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
            if (pendingConnect != null) {
                promise.tryFailure(new ConnectionPendingException());
                return;
            }
            if (active) {
                promise.tryFailure(new AlreadyConnectedException());
                return;
            }
            pendingConnect = promise;
            parentConnected = false;
            try {
                registerParent();
                final ChannelFuture future = parent().connect(remoteAddress, localAddress);
                future.addListener(RakNet.INTERNAL_WRITE_LISTENER);
                future.addListener((ChannelFutureListener) result -> {
                    try {
                        eventLoop().execute(() -> publishConnectResult(result));
                    } catch (RejectedExecutionException failure) {
                        promise.tryFailure(failure);
                        parent().close();
                    }
                });
            } catch (RuntimeException failure) {
                failPendingConnect(failure);
            }
        }
    }

    /**
     * Records the parent's result on the application loop without reporting success before activation.
     *
     * @param result completed UDP and RakNet negotiation future
     */
    private void publishConnectResult(final ChannelFuture result) {
        if (!result.isSuccess()) {
            failPendingConnect(result.cause());
        } else if (!isOpen()) {
            failPendingConnect(new ClosedChannelException());
        } else {
            parentConnected = true;
            completePendingConnect();
        }
    }

    /** Completes application connection only after both successful negotiation and visible activation. */
    private void completePendingConnect() {
        if (pendingConnect != null && parentConnected && active) {
            final ChannelPromise promise = pendingConnect;
            pendingConnect = null;
            promise.trySuccess();
        }
    }

    /**
     * Fails an unfinished application connection and drops its ownership before invoking listeners.
     *
     * @param cause connection or closure failure reported to the caller
     */
    private void failPendingConnect(final Throwable cause) {
        parentConnected = false;
        if (pendingConnect != null) {
            final ChannelPromise promise = pendingConnect;
            pendingConnect = null;
            promise.tryFailure(cause);
        }
    }

    /**
     * Registers the parent once using the explicit IO loop or the remembered application loop.
     * The application must already have selected a loop unless the caller provided a parent loop explicitly.
     *
     * @throws IllegalStateException if neither registration nor an explicit choice has supplied a parent loop
     */
    private void registerParent() {
        if (!parent().isRegistered()) {
            EventLoop loop = this.providedParentEventLoop != null ? this.providedParentEventLoop : this.pendingEventLoop;
            if (loop == null) {
                throw new IllegalStateException("channel not registered");
            }
            loop.register(parent());
        }
    }

    /**
     * Relays parent transport events into the application pipeline without duplicating message references.
     */
    protected class ParentReadHandler extends ChannelInboundHandlerAdapter {

        /**
         * Publishes activity and fires the active event on the application event loop.
         *
         * @param ctx parent transport context that received the event
         */
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            RakNetClientThreadedChannel.this.eventLoop().execute(() -> {
                RakNetClientThreadedChannel.this.active = true;
                RakNetClientThreadedChannel.this.pipeline().fireChannelActive();
                completePendingConnect();
            });
        }

        /**
         * Clears activity and fires the inactive event on the application event loop.
         *
         * @param ctx parent transport context that received the event
         */
        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            RakNetClientThreadedChannel.this.eventLoop().execute(() -> {
                RakNetClientThreadedChannel.this.active = false;
                failPendingConnect(new ClosedChannelException());
                RakNetClientThreadedChannel.this.pipeline().fireChannelInactive();
            });
        }

        /**
         * Transfers an inbound message into the application pipeline without an extra retain or release.
         * Netty dispatches application handlers according to their configured executors.
         *
         * @param ctx parent transport context delivering the message
         * @param msg message reference transferred to the application pipeline
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            RakNetClientThreadedChannel.this.pipeline().fireChannelRead(msg);
        }

        /**
         * Signals completion of the current parent read batch through the application pipeline.
         *
         * @param ctx parent transport context that received the event
         */
        @Override
        public void channelReadComplete(final ChannelHandlerContext ctx) {
            RakNetClientThreadedChannel.this.pipeline().fireChannelReadComplete();
        }

        /**
         * Forwards a parent user event to application handlers without interpreting or retaining it.
         *
         * @param ctx parent transport context receiving the event
         * @param evt event whose ownership transfers to the application pipeline
         */
        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
            RakNetClientThreadedChannel.this.pipeline().fireUserEventTriggered(evt);
        }

        /**
         * Relays a parent pressure change so application handlers can recheck combined writability.
         *
         * @param ctx parent transport context that received the event
         */
        @Override
        public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
            RakNetClientThreadedChannel.this.pipeline().fireChannelWritabilityChanged();
        }

        /**
         * Forwards a transport exception to the application's exception-handling pipeline.
         *
         * @param ctx parent context reporting the failure
         * @param cause transport failure delivered to application handlers
         */
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            RakNetClientThreadedChannel.this.pipeline().fireExceptionCaught(cause);
        }

    }

}
