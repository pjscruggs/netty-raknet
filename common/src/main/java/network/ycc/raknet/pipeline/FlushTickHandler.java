package network.ycc.raknet.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

/**
 * This handler produces an automatic flush cycle that is driven by the channel IO itself. The ping
 * produced in {@link AbstractConnectionInitializer} serves as a timed driver if no IO is present.
 * The channel write signal is driven by {@link ReliabilityHandler} using {@link
 * FlushTickHandler#checkFlushTick(Channel)}.
 */
public class FlushTickHandler extends ChannelDuplexHandler {

    /** Pipeline name for the connection's elapsed-time flush accounting. */
    public static final String NAME = "rn-flush-tick";
    /** Five-millisecond unit used to charge flushes and report missed scheduling intervals. */
    public static final long TICK_RESOLUTION = TimeUnit.NANOSECONDS.convert(5, TimeUnit.MILLISECONDS);

    /** Fired near the end of a pipeline to trigger a flush check. */
    public static final Object FLUSH_CHECK_SIGNAL = new Object();

    /** Elapsed nanoseconds not yet consumed by flushes or missed-interval notifications. */
    protected long tickAccum = 0;
    /** Monotonic timestamp of the previous elapsed-time accounting update. */
    protected long lastTickAccum = System.nanoTime();
    /** Owned fallback task that checks for due flushes every fifty milliseconds. */
    protected ScheduledFuture<?> flushTask = null;

    /**
     * Requests elapsed-time accounting through the pipeline's internal user-event path.
     *
     * @param channel connection whose flush handler should check its accumulated interval
     */
    public static void checkFlushTick(final Channel channel) {
        channel.pipeline().fireUserEventTriggered(FLUSH_CHECK_SIGNAL);
    }

    /**
     * Schedules fallback flush checks on the owning event loop for periods without incoming or outgoing traffic.
     *
     * @param ctx connection whose flush schedule is owned by this handler
     */
    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) {
        assert flushTask == null;
        flushTask = ctx.channel().eventLoop().scheduleAtFixedRate(
                () -> checkFlushTick(ctx.channel()),
                0, 50, TimeUnit.MILLISECONDS
        );
    }

    /**
     * Cancels the owned fallback timer without interrupting an event-loop callback already running.
     *
     * @param ctx pipeline context being detached
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) {
        flushTask.cancel(false);
        flushTask = null;
    }

    /**
     * Forwards read completion before checking whether elapsed time warrants an outbound flush.
     *
     * @param ctx connection completing its current inbound read batch
     */
    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        ctx.fireChannelReadComplete();
        maybeFlush(ctx.channel());
    }

    /**
     * Consumes the internal flush-check signal and forwards every unrelated user event unchanged.
     *
     * @param ctx connection whose elapsed-time balance may trigger a flush
     * @param evt internal check signal or event owned by another pipeline stage
     */
    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
        if (evt == FLUSH_CHECK_SIGNAL) {
            maybeFlush(ctx.channel());
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    /**
     * Charges one flush interval against the elapsed-time balance, then forwards the actual flush.
     *
     * @param ctx outbound context through which the flush continues
     */
    @Override
    public void flush(final ChannelHandlerContext ctx) {
        if (tickAccum >= TICK_RESOLUTION) {
            tickAccum -= TICK_RESOLUTION;
        } else {
            tickAccum = 0;
        }
        ctx.flush();
    }

    /**
     * Checks for a due flush before propagating the channel's new backpressure state.
     *
     * @param ctx connection whose writable state changed
     */
    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) {
        maybeFlush(ctx.channel());
        ctx.fireChannelWritabilityChanged();
    }

    /**
     * Accumulates monotonic elapsed time, flushes when due and reports remaining missed intervals.
     * The missed-interval event lets reliability scheduling recover its transmission allowance after a pause.
     *
     * @param channel connection whose event loop owns this handler's timing state
     */
    protected void maybeFlush(final Channel channel) {
        final long curTime = System.nanoTime();
        tickAccum += curTime - lastTickAccum;
        lastTickAccum = curTime;

        if (tickAccum >= TICK_RESOLUTION) {
            channel.flush();

            final int nFlushes = (int) (tickAccum / TICK_RESOLUTION);
            if (nFlushes > 0) {
                tickAccum -= nFlushes * TICK_RESOLUTION;
                channel.pipeline().fireUserEventTriggered(new MissedFlushes(nFlushes));
            }
        }
    }

    /** Fired from this handler to alert handlers of a missed flush tick. */
    public static class MissedFlushes {
        /** Number of overdue flush intervals available to restore the receiver's burst allowance. */
        public final int nFlushes;

        /**
         * Captures the elapsed intervals without retaining the originating handler or connection.
         *
         * @param nFlushes positive number of missed flush intervals
         */
        public MissedFlushes(final int nFlushes) {
            this.nFlushes = nFlushes;
        }
    }

}
