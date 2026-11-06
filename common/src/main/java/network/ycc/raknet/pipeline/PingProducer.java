package network.ycc.raknet.pipeline;

import network.ycc.raknet.packet.Ping;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

public class PingProducer implements ChannelHandler {

    public static final String NAME = "rn-ping-producer";

    ScheduledFuture<?> pingTask = null;

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) {
        pingTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                () -> ctx.writeAndFlush(new Ping()),
                0, 200, TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) {
        if (pingTask != null) {
            pingTask.cancel(false);
            pingTask = null;
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        ctx.fireExceptionCaught(cause);
    }

}
