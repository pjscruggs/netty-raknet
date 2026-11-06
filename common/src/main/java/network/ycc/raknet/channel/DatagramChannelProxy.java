package network.ycc.raknet.channel;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultConfig;

import java.lang.reflect.InvocationTargetException;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.function.Supplier;

public class DatagramChannelProxy implements Channel {

    public static final String LISTENER_HANDLER_NAME = "rn-udp-listener-handler";

    protected final Supplier<? extends DatagramChannel> ioChannelSupplier;
    protected final DefaultChannelPipeline pipeline;
    protected final DatagramChannel listener;
    protected final Config config;

    public DatagramChannelProxy(Supplier<? extends DatagramChannel> ioChannelSupplier) {
        this.ioChannelSupplier = ioChannelSupplier;
        listener = ioChannelSupplier.get();
        pipeline = newChannelPipeline();
        listener.pipeline().addLast(LISTENER_HANDLER_NAME, new ListenerInboundProxy());
        pipeline()
                .addLast(LISTENER_HANDLER_NAME, new ListnerOutboundProxy())
                .addLast(new FlushConsolidationHandler(
                        256, true));
        config = new Config();
    }

    public DatagramChannelProxy(Class<? extends DatagramChannel> ioChannelType) {
        this(() -> {
            try {
                return ioChannelType.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalArgumentException("Failed to create instance", e);
            }
        });
    }

    @Override
    public ChannelId id() {
        return listener.id();
    }

    @Override
    public EventLoop eventLoop() {
        return listener.eventLoop();
    }

    @Override
    public Channel parent() {
        return listener;
    }

    @Override
    public RakNet.Config config() {
        return config;
    }

    @Override
    public boolean isOpen() {
        return listener.isOpen();
    }

    @Override
    public boolean isRegistered() {
        return listener.isRegistered();
    }

    @Override
    public boolean isActive() {
        return listener.isActive();
    }

    @Override
    public ChannelMetadata metadata() {
        return listener.metadata();
    }

    @Override
    public SocketAddress localAddress() {
        return listener.localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return listener.remoteAddress();
    }

    @Override
    public ChannelFuture closeFuture() {
        return listener.closeFuture();
    }

    @Override
    public boolean isWritable() {
        return listener.isWritable();
    }

    @Override
    public long bytesBeforeUnwritable() {
        return listener.bytesBeforeUnwritable();
    }

    @Override
    public long bytesBeforeWritable() {
        return listener.bytesBeforeWritable();
    }

    @Override
    public Unsafe unsafe() {
        return listener.unsafe();
    }

    @Override
    public ChannelPipeline pipeline() {
        return pipeline;
    }

    @Override
    public ByteBufAllocator alloc() {
        return config().getAllocator();
    }

    @Override
    public Channel read() {
        // NOOP
        return this;
    }

    @Override
    public Channel flush() {
        pipeline.flush();
        return this;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return pipeline.bind(localAddress);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return pipeline.connect(remoteAddress);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return pipeline.connect(remoteAddress, localAddress);
    }

    @Override
    public ChannelFuture disconnect() {
        return pipeline.disconnect();
    }

    @Override
    public ChannelFuture close() {
        return pipeline.close();
    }

    @Override
    public ChannelFuture deregister() {
        return pipeline.deregister();
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return pipeline.bind(localAddress, promise);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return pipeline.connect(remoteAddress, promise);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) {
        return pipeline.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
        return pipeline.disconnect(promise);
    }

    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return pipeline.close(promise);
    }

    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        return pipeline.deregister(promise);
    }

    @Override
    public ChannelFuture write(Object msg) {
        return pipeline.write(msg);
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        return pipeline.write(msg, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return pipeline.writeAndFlush(msg, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        return pipeline.writeAndFlush(msg);
    }

    @Override
    public ChannelPromise newPromise() {
        return pipeline.newPromise();
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return pipeline.newProgressivePromise();
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return pipeline.newSucceededFuture();
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return pipeline.newFailedFuture(cause);
    }

    @Override
    public ChannelPromise voidPromise() {
        return pipeline.voidPromise();
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return listener.attr(key);
    }

    @Override
    public <T> boolean hasAttr(AttributeKey<T> key) {
        return listener.hasAttr(key);
    }

    @Override
    public int compareTo(Channel o) {
        return listener.compareTo(o);
    }

    protected void gracefulClose(ChannelPromise promise) {
        listener.close(wrapPromise(promise));
    }

    protected DefaultChannelPipeline newChannelPipeline() {
        return new DefaultChannelPipeline(this) {
            @Override
            protected void onUnhandledInboundException(Throwable cause) {
                if (cause instanceof ClosedChannelException) {
                    ReferenceCountUtil.safeRelease(cause);
                    return;
                }
                super.onUnhandledInboundException(cause);
            }
        };
    }

    protected ChannelPromise wrapPromise(ChannelPromise in) {
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

    protected class Config extends DefaultConfig {

        protected Config() {
            super(DatagramChannelProxy.this);
        }

        @Override
        @SuppressWarnings({"unchecked", "deprecation"})
        public <T> T getOption(ChannelOption<T> option) {
            final T thisOption = super.getOption(option);
            if (thisOption == null) {
                return listener.config().getOption(option);
            }
            return thisOption;
        }

        @Override
        @SuppressWarnings("deprecation")
        public <T> boolean setOption(ChannelOption<T> option, T value) {
            final boolean thisOption = super.setOption(option, value);
            final boolean listenOption = listener.config().setOption(option, value);
            return thisOption || listenOption;
        }

    }

    protected class ListnerOutboundProxy implements ChannelOutboundHandler {

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            assert listener.eventLoop().inEventLoop();
        }

        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress,
                ChannelPromise promise) {
            listener.bind(localAddress, wrapPromise(promise));
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                SocketAddress localAddress, ChannelPromise promise) {
            listener.connect(remoteAddress, localAddress, wrapPromise(promise));
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) {
            listener.disconnect(wrapPromise(promise));
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
            gracefulClose(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) {
            listener.deregister(wrapPromise(promise));
        }

        @Override
        public void read(ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            listener.write(msg, wrapPromise(promise));
        }

        @Override
        public void flush(ChannelHandlerContext ctx) {
            listener.flush();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof NoRouteToHostException) {
                return;
            }
            ctx.fireExceptionCaught(cause);
        }

    }

    protected class ListenerInboundProxy implements ChannelInboundHandler {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
            pipeline.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) {
            pipeline.fireChannelUnregistered();
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            assert listener.eventLoop().inEventLoop();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // NOOP - active status managed by connection sequence
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            pipeline.fireChannelInactive();
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            // NOOP
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            pipeline.fireChannelRead(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            pipeline.fireChannelReadComplete();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            // NOOP
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            pipeline.fireChannelWritabilityChanged();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof ClosedChannelException) {
                return;
            }
            pipeline.fireExceptionCaught(cause);
        }

    }

}
