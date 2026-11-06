package network.ycc.raknet.pipeline;

import network.ycc.raknet.packet.Ping;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

/** Schedules connection-local transport pings every 200 milliseconds on the owning event loop. */
public class PingProducer implements ChannelHandler {

    /** Pipeline name used to ensure a connection has only one periodic ping producer. */
    public static final String NAME = "rn-ping-producer";

    /** Owned periodic task, or null before installation and after cancellation. */
    ScheduledFuture<?> pingTask;

    /**
     * Starts periodic ping writes immediately, using the same event loop that owns the transport pipeline.
     *
     * @param ctx connection used for scheduling and flushing pings
     */
    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) {
        pingTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                () -> ctx.writeAndFlush(new Ping()),
                0, 200, TimeUnit.MILLISECONDS
        );
    }

    /**
     * Cancels the owned ping schedule without interrupting an event-loop callback already executing.
     *
     * @param ctx context being detached from the connection pipeline
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) {
        if (pingTask != null) {
            pingTask.cancel(false);
            pingTask = null;
        }
    }

    /**
     * Forwards pipeline failures so the connection's normal error handling remains responsible for closure.
     *
     * @param ctx context from which the exception is forwarded
     * @param cause original failure, passed onward unchanged
     */
    @SuppressWarnings("deprecation")
    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        ctx.fireExceptionCaught(cause);
    }

}
