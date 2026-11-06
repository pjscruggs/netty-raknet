package network.ycc.raknet.server.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
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

public class RakNetChildChannel extends AbstractChannel {

    public static final String WRITE_HANDLER_NAME = "rn-child-channel-write-handler";

    private static final ChannelMetadata metadata = new ChannelMetadata(false);
    protected final ChannelPromise connectPromise;
    protected final ChannelPromise closePromise;
    protected final RakNet.Config config;
    protected final InetSocketAddress remoteAddress;
    protected final InetSocketAddress localAddress;
    protected final RakNetApplicationChannel applicationChannel;
    protected final AtomicBoolean isRegisteringApplicationChannel = new AtomicBoolean(false);
    protected final Queue<Runnable> pendingApplicationChannelOperations = new LinkedList<>();
    protected final Consumer<Channel> registerChannel;
    private final DatagramChannel listener;

    protected volatile boolean open = true;

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

    protected void addDefaultPipeline() {
        pipeline().addLast(RakNetServer.DefaultChildInitializer.INSTANCE);
        connectPromise.addListener(x2 -> {
            if (!x2.isSuccess()) {
                RakNetChildChannel.this.applicationChannel.close();
                RakNetChildChannel.this.close();
            }
        });
        pipeline().addLast(new ChannelInitializer<RakNetChildChannel>() {
            @Override
            protected void initChannel(final RakNetChildChannel ch) {
                pipeline().replace(ConnectionInitializer.NAME, ConnectionInitializer.NAME,
                        new ConnectionInitializer(connectPromise));
                pipeline().addLast(RakNetApplicationChannel.NAME_SERVER_PARENT_THREADED_READ_HANDLER, new ReadHandler());
            }
        });
    }

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

    public ChannelFuture connectFuture() {
        return connectPromise;
    }

    @Override
    public boolean isWritable() {
        final Boolean result = attr(RakNet.WRITABLE).get();
        return (result == null || result) && parent().isWritable();
    }

    @Override
    public long bytesBeforeUnwritable() {
        return parent().bytesBeforeUnwritable();
    }

    @Override
    public long bytesBeforeWritable() {
        return parent().bytesBeforeWritable();
    }

    @Override
    public RakNetServerChannel parent() {
        return (RakNetServerChannel) super.parent();
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new AbstractUnsafe() {
            @Override
            public void connect(final SocketAddress addr1, final SocketAddress addr2, final ChannelPromise pr) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected boolean isCompatible(final EventLoop eventloop) {
        return true;
    }

    @Override
    protected SocketAddress localAddress0() {
        return localAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return remoteAddress;
    }

    @Override
    protected void doBind(final SocketAddress addr) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doDisconnect() {
        close();
    }

    @Override
    protected void doClose() {
        open = false;
        if (this.applicationChannel.isRegistered()) {
            this.applicationChannel.close();
        }
    }

    @Override
    protected void doBeginRead() {
        // NOOP
    }

    @Override
    protected void doWrite(final ChannelOutboundBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RakNet.Config config() {
        return config;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isActive() {
        return isOpen() && parent().isActive() && connectPromise.isSuccess();
    }

    @Override
    public ChannelMetadata metadata() {
        return metadata;
    }

    public RakNetApplicationChannel getApplicationChannel() {
        return this.applicationChannel;
    }

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
                    if (future1.isDone()) {
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

    protected class WriteHandler extends ChannelOutboundHandlerAdapter {
        protected boolean needsFlush = false;

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

        @Override
        public void flush(final ChannelHandlerContext ctx) {
            if (needsFlush) {
                needsFlush = false;
                parent().flush();
            }
        }

        @Override
        public void read(final ChannelHandlerContext ctx) {
            // NOOP
        }
    }

    protected class ReadHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            registerApplicationChannelIfNecessary();
            initStandaloneListener().addListener(future -> {
                if (applicationChannel.isRegistered()) {
                    channelActive0();
                } else {
                    pendingApplicationChannelOperations.add(this::channelActive0);
                }
            });
        }

        private void channelActive0() {
            applicationChannel.eventLoop().execute(() -> {
                applicationChannel.setActiveInternal(true);
                applicationChannel.pipeline().fireChannelActive();
            });
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                channelInactive0();
            } else {
                pendingApplicationChannelOperations.add(this::channelInactive0);
            }
        }

        private void channelInactive0() {
            applicationChannel.eventLoop().execute(() -> {
                applicationChannel.setActiveInternal(false);
                applicationChannel.pipeline().fireChannelInactive();
            });
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                applicationChannel.pipeline().fireChannelRead(msg);
            } else {
                pendingApplicationChannelOperations.add(() -> applicationChannel.pipeline().fireChannelRead(msg));
            }
        }

        @Override
        public void channelReadComplete(final ChannelHandlerContext ctx) {
            if (!applicationChannel.isRegistered()) return;
            applicationChannel.pipeline().fireChannelReadComplete();
        }

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

        @Override
        public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
            registerApplicationChannelIfNecessary();
            if (applicationChannel.isRegistered()) {
                applicationChannel.pipeline().fireChannelWritabilityChanged();
            } else {
                pendingApplicationChannelOperations.add(() -> applicationChannel.pipeline().fireChannelWritabilityChanged());
            }
        }

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

    protected class ListenerOutboundProxy implements ChannelOutboundHandler {

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
            assert listener.eventLoop().inEventLoop();
        }

        @Override
        public void bind(final ChannelHandlerContext ctx, final SocketAddress localAddress,
                         final ChannelPromise promise) {
//            listener.bind(localAddress, wrapPromise(promise));
            promise.setFailure(new UnsupportedOperationException());
        }

        @Override
        public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                            final SocketAddress localAddress, final ChannelPromise promise) {
//            listener.connect(remoteAddress, localAddress, wrapPromise(promise));
            promise.setFailure(new UnsupportedOperationException());
        }

        @Override
        public void handlerRemoved(final ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void disconnect(final ChannelHandlerContext ctx, final ChannelPromise promise) {
//            listener.disconnect(wrapPromise(promise));
            promise.setFailure(new UnsupportedOperationException());
        }

        @Override
        public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) {
            if (listener.isRegistered()) {
                ChannelPromise listenerClose = ctx.newPromise();
                listener.close(wrapPromise(listenerClose));
                listenerClose.addListener(future -> {
                    if (!future.isSuccess()) {
                        future.cause().printStackTrace();
                    }
                    ctx.close(promise);
                });
            } else {
                ctx.close(promise);
            }
        }

        @Override
        public void deregister(final ChannelHandlerContext ctx, final ChannelPromise promise) {
            ctx.deregister(promise);
        }

        @Override
        public void read(final ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
            listener.write(msg, wrapPromise(promise)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
        }

        @Override
        public void flush(final ChannelHandlerContext ctx) {
            listener.flush();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (cause instanceof NoRouteToHostException) {
                return;
            }
            ctx.fireExceptionCaught(cause);
        }

    }

    protected class ListenerInboundProxy implements ChannelInboundHandler {

        @Override
        public void channelRegistered(final ChannelHandlerContext ctx) {
        }

        @Override
        public void channelUnregistered(final ChannelHandlerContext ctx) {
        }

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
            assert listener.eventLoop().inEventLoop();
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            // NOOP - active status managed by connection sequence
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
        }

        @Override
        public void handlerRemoved(final ChannelHandlerContext ctx) {
            // NOOP
        }

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

        @Override
        public void channelReadComplete(final ChannelHandlerContext ctx) {
            pipeline().fireChannelReadComplete();
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
            // NOOP
        }

        @Override
        public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
            pipeline().fireChannelWritabilityChanged();
        }

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
