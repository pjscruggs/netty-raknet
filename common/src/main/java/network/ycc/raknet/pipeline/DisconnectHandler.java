package network.ycc.raknet.pipeline;

import network.ycc.raknet.packet.Disconnect;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * Exchanges a transport disconnect notice before closing an active connection.
 * Per-close promises and timers are local to the callback, so the handler can be shared.
 */
@ChannelHandler.Sharable
public class DisconnectHandler extends ChannelDuplexHandler {

    /** Pipeline name for graceful transport closure above reliability processing. */
    public static final String NAME = "rn-disconnect";
    /** Shared stateless handler for inbound notices and outbound close requests. */
    public static final DisconnectHandler INSTANCE = new DisconnectHandler();

    /**
     * Consumes a peer disconnect, removes the close interceptor, and flushes pending acknowledgements before closure.
     * Other messages pass downstream with their existing ownership unchanged.
     *
     * @param ctx pipeline from which the disconnect interceptor can remove itself
     * @param msg message consumed on disconnect or transferred to the next inbound handler
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof Disconnect) {
            ReferenceCountUtil.release(msg);
            ctx.pipeline().remove(this);
            ctx.channel().flush().close(); //send ACKs
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    /**
     * Sends a disconnect notice before completing the caller's close request on an active channel.
     * Notice completion or a three-second deadline triggers closure. Inactive channels close immediately.
     *
     * @param ctx connection used for the notice, deadline and eventual close
     * @param promise caller's promise completed by the underlying channel close
     */
    @Override
    public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (ctx.channel().isActive()) {
            final ChannelPromise disconnectPromise = ctx.newPromise();
            final ScheduledFuture<?> timeout = ctx.channel().eventLoop().schedule(
                    () -> {
                        disconnectPromise.trySuccess();
                        throw new SocketTimeoutException();
                    }, 3, TimeUnit.SECONDS); //TODO: config
            ctx.channel().writeAndFlush(new Disconnect())
                    .addListener(f -> disconnectPromise.trySuccess());
            disconnectPromise.addListener(f -> {
                timeout.cancel(false);
                ctx.close(promise);
            });
        } else {
            ctx.close(promise);
        }
    }

}
