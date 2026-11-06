package network.ycc.raknet.server.channel;

import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.util.concurrent.PromiseCombiner;
import network.ycc.raknet.RakNet;

import java.net.SocketAddress;

/**
 * Exposes an application pipeline separately from the parent RakNet transport pipeline.
 * Writes and configuration are delegated to the parent. Application handlers may use a different event loop,
 * while the parent retains responsibility for datagram IO, reliability and connection negotiation.
 */
public class RakNetApplicationChannel extends AbstractChannel {

    /**
     * Parent-pipeline name of the bridge that delivers established transport events to the application.
     */
    public static final String NAME_SERVER_PARENT_THREADED_READ_HANDLER = "rn-server-parent-threaded-read-handler";

    /**
     * Application-visible activity state, published when parent lifecycle events reach the application loop.
     */
    private volatile boolean active = false;

    /**
     * Creates the application-facing channel for an existing server-side peer transport.
     *
     * @param parent child connection retaining socket and protocol ownership
     */
    protected RakNetApplicationChannel(final RakNetChildChannel parent) {
        super(parent);
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
    public RakNetChildChannel parent() {
        return (RakNetChildChannel) super.parent();
    }

    /**
     * Creates the channel-internal bridge that redirects writes and lifecycle operations to the parent.
     *
     * @return connection-local unsafe implementation used by AbstractChannel
     */
    @Override
    protected AbstractUnsafe newUnsafe() {
        return new RakNetApplicationChannelUnsafe();
    }

    /**
     * Returns the intercepting unsafe facade rather than exposing AbstractChannel's default outbound path.
     *
     * @return facade that sends writes through the parent and coordinates parent/application closure
     */
    @Override
    public Unsafe unsafe() {
        return ((RakNetApplicationChannelUnsafe) super.unsafe()).wrapped;
    }

    /**
     * Accepts any application event loop because the parent retains all socket IO requirements.
     *
     * @param eventloop application event loop proposed during registration
     * @return true for every event loop supplied to this application wrapper
     */
    @Override
    protected boolean isCompatible(final EventLoop eventloop) {
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
        parent().close();
    }

    /**
     * Marks application activity false and initiates closure of the transport that owns its resources.
     */
    @Override
    protected void doClose() {
//        close();
        this.active = false;
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
     * Publishes the lifecycle state selected by the parent bridge before firing the corresponding application event.
     *
     * @param active new application activity state
     */
    void setActiveInternal(final boolean active) {
        this.active = active;
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
     * Closes through the application pipeline once registered, or directly through the parent before registration.
     * The fallback avoids asking an unregistered application channel for an event-loop-bound close operation.
     *
     * @return future for the close path that owns the connection at the time of the call
     */
    @Override
    public ChannelFuture close() {
        if (this.isRegistered()) {
            return super.close();
        } else {
            return parent().close();
        }
    }

    /**
     * Coordinates application-channel operations with the parent while bypassing a second outbound buffer.
     */
    protected final class RakNetApplicationChannelUnsafe extends AbstractUnsafe {

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
                return RakNetApplicationChannelUnsafe.this.recvBufAllocHandle();
            }

            /**
             * Resolves the application address through the enclosing unsafe implementation and parent transport.
             *
             * @return currently bound local endpoint, or null before binding
             */
            @Override
            public SocketAddress localAddress() {
                return RakNetApplicationChannelUnsafe.this.localAddress();
            }

            /**
             * Resolves the application peer through the enclosing unsafe implementation and parent transport.
             *
             * @return selected remote endpoint, or null before connection
             */
            @Override
            public SocketAddress remoteAddress() {
                return RakNetApplicationChannelUnsafe.this.remoteAddress();
            }

            /**
             * Registers the application pipeline with its selected event loop without registering a second socket.
             *
             * @param eventLoop loop that will execute this application's handlers
             * @param promise completed by AbstractUnsafe when registration succeeds or fails
             */
            @Override
            public void register(final EventLoop eventLoop, final ChannelPromise promise) {
                RakNetApplicationChannelUnsafe.this.register(eventLoop, promise);
            }

            /**
             * Passes a direct bind attempt to AbstractUnsafe, whose application bind hook rejects socket rebinding.
             *
             * @param localAddress requested address that cannot be bound by this wrapper
             * @param promise receives the unsupported-operation failure
             */
            @Override
            public void bind(final SocketAddress localAddress, final ChannelPromise promise) {
                RakNetApplicationChannelUnsafe.this.bind(localAddress, promise);
            }

            /**
             * Passes a direct connect attempt to the enclosing unsafe implementation, which rejects new connections.
             *
             * @param remoteAddress requested peer address
             * @param localAddress requested local bind address
             * @param promise caller's connection promise, unused by the rejecting implementation
             * @throws UnsupportedOperationException because a server application channel already belongs to an accepted peer
             */
            @Override
            public void connect(final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
                RakNetApplicationChannelUnsafe.this.connect(remoteAddress, localAddress, promise);
            }

            /**
             * Runs AbstractUnsafe's disconnect lifecycle, which clears activity and requests parent closure.
             *
             * @param promise completion of the application's disconnect operation
             */
            @Override
            public void disconnect(final ChannelPromise promise) {
                RakNetApplicationChannelUnsafe.this.disconnect(promise);
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
                            RakNetApplicationChannelUnsafe.this.close(newPromise);
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
                RakNetApplicationChannelUnsafe.this.closeForcibly();
            }

            /**
             * Deregisters this application pipeline through AbstractUnsafe without transferring socket ownership.
             *
             * @param promise completed when application deregistration finishes
             */
            @Override
            public void deregister(final ChannelPromise promise) {
                RakNetApplicationChannelUnsafe.this.deregister(promise);
            }

            /**
             * Invokes the application read hook, which is a no-op because the parent supplies inbound events.
             */
            @Override
            public void beginRead() {
                RakNetApplicationChannelUnsafe.this.beginRead();
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
                return RakNetApplicationChannelUnsafe.this.voidPromise();
            }

            /**
             * Exposes AbstractUnsafe's bookkeeping buffer, although ordinary writes bypass it through the facade.
             *
             * @return the underlying outbound buffer, which may be null after closure
             */
            @Override
            public ChannelOutboundBuffer outboundBuffer() {
                return RakNetApplicationChannelUnsafe.this.outboundBuffer();
            }
        };

        /**
         * Rejects connecting an application wrapper that was created for an already accepted server peer.
         *
         * @param remoteAddress requested peer address
         * @param localAddress requested local bind address
         * @param promise caller's promise, not completed before this method throws
         * @throws UnsupportedOperationException on every direct connection attempt
         */
        @Override
        public void connect(final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {
            throw new UnsupportedOperationException();
        }
    }

}
