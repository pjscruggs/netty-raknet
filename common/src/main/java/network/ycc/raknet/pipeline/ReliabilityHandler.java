package network.ycc.raknet.pipeline;

import java.util.Queue;
import java.nio.channels.ClosedChannelException;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ReferenceCountUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.pipeline.FlushTickHandler.MissedFlushes;
import network.ycc.raknet.utils.UINT;

import java.util.PriorityQueue;

/**
 * Owns frame queues, acknowledgement processing and retransmission scheduling for one connection.
 * Mutable counters and collections are confined to its event loop. Outgoing frames remain retained
 * until acknowledgement, recall or connection cleanup transfers or releases their ownership.
 */
public class ReliabilityHandler extends ChannelDuplexHandler {

    /** Pipeline name for frame-set acknowledgements, retransmission and outgoing queue ownership. */
    public static final String NAME = "rn-reliability";

    /** Maximum recent sequence positions tracked for advisory negative acknowledgements. */
    static final int MAX_NACK_WINDOW = 8192;

    /** Maximum adaptive retry wait, below the normal connection liveness timeout. */
    private static final long MAX_RETRY_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    /** Retry grace learned from ACK arrival independently of ping responses. */
    private long acknowledgementTimeout;

    /** Missing received datagram counters awaiting a negative acknowledgement when that feature is enabled. */
    protected final IntSortedSet nackSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    /** Received datagram counters awaiting acknowledgement, ordered by circular counter distance. */
    protected final IntSortedSet ackSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
    /** Owned unsent or recalled frame references waiting for packet production. */
    protected final Queue<Frame> frameQueue = new PriorityQueue<>(Frame.COMPARATOR);
    /** Owned transmitted frame sets awaiting acknowledgement, kept in send-time insertion order for expiry scans. */
    protected final Int2ObjectLinkedOpenHashMap<FrameSet> pendingFrameSets = new Int2ObjectLinkedOpenHashMap<>();

    /** Conservative total byte cost of frames waiting in frameQueue, excluding outstanding frame sets. */
    protected int queuedBytes;

    /** Newest received datagram sequence used to identify gaps with twenty-four-bit circular comparison. */
    protected int lastReceivedSeqId;
    /** Next twenty-four-bit sequence assigned to a newly produced outgoing frame set. */
    protected int nextSendSeqId;
    /** Clamped delivery trend that increases on acknowledgements and decreases when frames are recalled. */
    protected int resendGauge;
    /** Additional outstanding frame-set allowance above the configured baseline window. */
    protected int burstTokens;
    /** Connection configuration captured during installation for event-loop transport decisions. */
    protected RakNet.Config config; //TODO: not really needed anymore
    /** Installed context used by deferred frame production after acknowledgement callbacks. */
    protected ChannelHandlerContext ctx;

    /**
     * Reads the next datagram sequence on the owning event loop without exposing mutable scheduler state.
     *
     * @return twenty-four-bit identifier reserved for the next produced frame set
     */
    public final int nextSendSequence() {
        return nextSendSeqId;
    }

    /**
     * Advances receive-gap tracking after the application protocol abandons older datagrams.
     * Existing acknowledgements remain valid and a delayed marker cannot rewind the newest received sequence.
     *
     * @param sequence twenty-four-bit synchronization cursor
     * @throws IllegalArgumentException if the cursor cannot be represented on the wire
     */
    public final void skipReceivedThrough(final int sequence) {
        if (!UINT.fits(sequence, 24)) {
            throw new IllegalArgumentException("Invalid receive sequence");
        }
        if (UINT.B3.minusWrap(sequence, lastReceivedSeqId) > 0) {
            lastReceivedSeqId = sequence;
        }
        nackSet.removeIf(value -> UINT.B3.minusWrap(value, sequence) <= 0);
    }

    /**
     * Transfers all unsent and outstanding frame references to the caller for an event-loop synchronization pause.
     * Returned frames retain their original promises. The caller must restore or release every reference.
     *
     * @return independently owned list of detached frames, with no live scheduler collections exposed
     */
    public final java.util.List<Frame> takePendingFrames() {
        final java.util.List<Frame> frames = new java.util.ArrayList<>(frameQueue);
        frameQueue.clear();
        for (final FrameSet frameSet : pendingFrameSets.values()) {
            frameSet.createFrames(frames::add);
            frameSet.release();
        }
        pendingFrameSets.clear();
        queuedBytes = 0;
        config.getMetrics().currentQueuedBytes(0);
        return frames;
    }

    /**
     * Returns one paused frame to the scheduler on its event loop and updates queued-byte accounting.
     * Ownership transfers only after the queue accepts the frame. Its existing write promise is preserved.
     *
     * @param frame caller-owned retained frame from a synchronization pause
     */
    public final void restorePendingFrame(final Frame frame) {
        queueFrame(frame);
    }

    /** Deferred event-loop action that fills newly available send slots and flushes the resulting writes. */
    protected Runnable frameSetProduction = () -> {
        this.produceFrameSets(ctx);
        ctx.flush();
    };

    /**
     * Captures connection configuration and initializes the transport's application-visible writable state.
     *
     * @param ctx connection event loop and configuration owner
     */
    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) {
        config = RakNet.config(ctx);
        this.ctx = ctx;
        ctx.channel().attr(RakNet.WRITABLE).set(true);
        if (config.isIgnoreResendGauge()) {
            this.resendGauge = 2;
        }
    }

    /**
     * Releases all queued and outstanding frame references when this transport stage is detached.
     * Pending writers receive a closed-channel failure because detached frames can no longer be acknowledged.
     *
     * @param ctx context being removed from the pipeline
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) {
        clearQueue(new ClosedChannelException());
        ackSet.clear();
        nackSet.clear();
    }

    /**
     * Takes ownership of outgoing frames for reliable scheduling and associates their originating write promise.
     * Non-frame messages continue directly downstream. A flush check is requested after either path.
     *
     * @param ctx outbound connection context
     * @param msg frame to queue or other message to forward
     * @param promise write completion attached to a frame or forwarded with another message
     */
    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        if (msg instanceof Frame) {
            final Frame frame = (Frame) msg;
            try {
                queueFrame(frame);
                frame.setPromise(promise);
            } catch (CodecException failure) {
                frame.release();
                promise.tryFailure(failure);
                return;
            }
        } else {
            ctx.write(msg, promise);
        }
        FlushTickHandler.checkFlushTick(ctx.channel());
    }

    /**
     * Sends receipt responses, recalls timed-out frame sets and fills the current transmission window.
     * Queue limits and writable state are updated before forwarding the final transport flush.
     *
     * @param ctx connection whose event loop owns all queue and timing state
     */
    @Override
    public void flush(final ChannelHandlerContext ctx) {
        if (!ctx.channel().isOpen()) {
            ctx.flush();
            return;
        }
        //all data sent in order of priority
        sendResponses(ctx);
        recallExpiredFrameSets();
        produceFrameSets(ctx);
        updateBurstTokens(1);
        updateBackPressure(ctx);
        ctx.flush();
    }

    /**
     * Applies missed flush intervals to the send-window allowance, then forwards the original event.
     *
     * @param ctx connection receiving the scheduling notification
     * @param evt missed-interval notification or unrelated event to pass onward
     */
    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
        //missed some flush ticks, lets catch up on a few things
        if (evt instanceof MissedFlushes) {
            updateBurstTokens(((MissedFlushes) evt).nFlushes);
        }
        ctx.fireUserEventTriggered(evt);
    }

    /**
     * Consumes acknowledgements and received frame sets while balancing their incoming reference ownership.
     * Other messages are retained for downstream transfer before this handler releases its input reference.
     *
     * @param ctx inbound connection context
     * @param msg incoming packet or message owned by this callback
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        try {
            if (msg instanceof Reliability.ACK) {
                readAck((Reliability.ACK) msg);
            } else if (msg instanceof Reliability.NACK) {
                readNack((Reliability.NACK) msg);
            } else if (msg instanceof FrameSet) {
                readFrameSet(ctx, (FrameSet) msg);
            } else {
                ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    /**
     * Releases unsent frames and outstanding frame sets and resets queued-byte accounting.
     * When a cause is supplied, their attached write promises are failed before storage is released.
     *
     * @param t failure to report to pending writers, or null for cleanup without changing promise results
     */
    protected void clearQueue(final Throwable t) {
        if (t != null) {
            frameQueue.forEach(frame -> {
                if (frame.getPromise() != null) {
                    frame.getPromise().tryFailure(t);
                }
            });
            pendingFrameSets.values().forEach(set -> set.fail(t));
        }
        frameQueue.forEach(Frame::release);
        frameQueue.clear();
        this.queuedBytes = 0;
        pendingFrameSets.values().forEach(FrameSet::release);
        pendingFrameSets.clear();
    }

    /**
     * Records receipt and forwards frames without walking gaps when negative acknowledgements are off.
     *
     * @param ctx transport pipeline receiving the frame set
     * @param frameSet packet whose payload references are retained for downstream handlers
     */
    protected void readFrameSet(final ChannelHandlerContext ctx, final FrameSet frameSet) {
        final int packetSeqId = frameSet.getSeqId();
        ackSet.add(packetSeqId);
        if (config.isNACKEnabled()) {
            nackSet.remove(packetSeqId);
        }
        if (UINT.B3.minusWrap(packetSeqId, lastReceivedSeqId) > 0) {
            if (config.isNACKEnabled()) {
                queueMissingSequences(packetSeqId);
            }
            lastReceivedSeqId = packetSeqId;
        }
        config.getMetrics().packetsIn(1);
        config.getMetrics().framesIn(frameSet.getNumPackets());
        frameSet.createFrames(ctx::fireChannelRead);
        trySendResponses(ctx);
        ctx.fireChannelReadComplete();
    }

    /**
     * Tracks only the newest bounded window of missing datagrams, including across counter wrap.
     * Negative acknowledgements accelerate retries but are not required for delivery. Older missing
     * datagrams still retry when the sender's acknowledgement timer expires. Bounding both the scan
     * and retained set prevents a peer-controlled gap from allocating millions of tree nodes.
     *
     * @param received newer sequence number ending the exclusive missing range
     */
    private void queueMissingSequences(final int received) {
        nackSet.removeIf(sequence -> {
            final int distance = UINT.B3.minusWrap(received, sequence);
            return distance <= 0 || distance > MAX_NACK_WINDOW;
        });
        final int missingCount = Math.min(
                UINT.B3.minusWrap(received, lastReceivedSeqId) - 1, MAX_NACK_WINDOW);
        for (int missing = UINT.B3.plus(received, -missingCount); missing != received;
                missing = UINT.B3.plus(missing, 1)) {
            nackSet.add(missing);
        }
    }

    /**
     * Completes only outstanding frame sets covered by the peer's ranges.
     *
     * @param ack received acknowledgement whose ranges may overlap or wrap
     */
    protected void readAck(final Reliability.ACK ack) {
        int ackdBytes = 0;
        for (final int id : AcknowledgementRanges.select(ack, pendingFrameSets.keySet())) {
            final FrameSet frameSet = pendingFrameSets.remove(id);
            if (frameSet != null) {
                acknowledgeTiming(frameSet);
                ackdBytes += frameSet.getRoughSize();
                adjustResendGauge(1);
                try {
                    frameSet.succeed();
                } finally {
                    frameSet.release();
                }
                tryProduceFrameSets();
            }
        }
        config.getMetrics().bytesACKd(ackdBytes);
    }

    /**
     * Recalls outstanding packets once even when the peer repeats broad or overlapping ranges.
     *
     * @param nack received negative acknowledgement
     */
    protected void readNack(final Reliability.NACK nack) {
        if (!config.isNACKEnabled()) {
            return;
        }

        int bytesNACKd = 0;
        for (final int id : AcknowledgementRanges.select(nack, pendingFrameSets.keySet())) {
            final FrameSet frameSet = pendingFrameSets.remove(id);
            if (frameSet != null) {
                bytesNACKd += frameSet.getRoughSize();
                recallFrameSet(frameSet);
            }
        }
        config.getMetrics().bytesNACKd(bytesNACKd);
    }

    /**
     * Accepts a frame reference into the unsent queue after checking its conservative size against the MTU.
     * No extra retain is taken. If validation fails before insertion, ownership remains with the caller.
     *
     * @param frame owned reference to transfer into the queue on success
     * @throws CorruptedFrameException if the estimated frame size exceeds the configured MTU
     * @throws TooLongFrameException if accepting the frame would exceed the unsent queue budget
     */
    protected void queueFrame(final Frame frame) {
        final long roughPacketSize = (long) frame.getDataSize() + Frame.HEADER_SIZE;
        if (roughPacketSize > config.getMTU()) {
            throw new CorruptedFrameException(
                    "Finished frame larger than the MTU by " + (roughPacketSize - config
                            .getMTU()));
        }
        if (roughPacketSize + queuedBytes > config.getMaxQueuedBytes()) {
            throw new TooLongFrameException("Unsent frame queue exceeds the connection budget");
        }
        frameQueue.add(frame);
        this.queuedBytes += (int) roughPacketSize;
        config.getMetrics().currentQueuedBytes(this.queuedBytes);
    }

    /**
     * Updates the clamped delivery trend, or fixes it positive when resend adaptation is disabled.
     *
     * @param n signed change contributed by an acknowledgement or recall
     */
    protected void adjustResendGauge(final int n) {
        if (config.isIgnoreResendGauge()) {
            this.resendGauge = 2;
            return;
        }
        //clamped gauge, can rebound more easily
        resendGauge = Math.max(
                -config.getDefaultPendingFrameSets(),
                Math.min(config.getDefaultPendingFrameSets(), resendGauge + n)
        );
    }

    /**
     * Adjusts additional send slots from recent delivery success and whether the previous allowance was used.
     * No-delay mode selects the configured maximum allowance without gradual adaptation.
     *
     * @param nTicks number of flush intervals contributing to the adjustment
     */
    protected void updateBurstTokens(final int nTicks) {
        if (config.isNoDelayEnabled()) {
            burstTokens = Math.max(0, config.getMaxPendingFrameSets());
        } else {
            // gradual increment or decrement for burst tokens, unless unused
            final boolean burstUnused = pendingFrameSets.size() < burstTokens / 2;
            if (resendGauge > 1 && !burstUnused) {
                burstTokens += 1 * nTicks;
            } else if (resendGauge < -1 || burstUnused) {
                burstTokens -= 3 * nTicks;
            }
            burstTokens = Math.max(Math.min(burstTokens, config.getMaxPendingFrameSets()), 0);
        }
        config.getMetrics().measureBurstTokens(burstTokens);
    }

    /**
     * Queues compressed ACK ranges and, when enabled and reading is active, NACK ranges.
     * Queued counters are cleared after their response packet is created. This method does not flush.
     *
     * @param ctx connection on which receipt responses are written
     */
    protected void sendResponses(final ChannelHandlerContext ctx) {
        if (!ackSet.isEmpty()) {
            ctx.write(new Reliability.ACK(ackSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().acksSent(ackSet.size());
            ackSet.clear();
        }
        if (config.isNACKEnabled() && !nackSet.isEmpty() && config.isAutoRead()) { //only nack if we can read
            ctx.write(new Reliability.NACK(nackSet)).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().nacksSent(nackSet.size());
            nackSet.clear();
        }
    }

    /**
     * Flushes receipt responses early when accumulated ACKs reach the configured baseline window threshold.
     *
     * @param ctx connection used to send and flush pending responses
     */
    protected void trySendResponses(final ChannelHandlerContext ctx) {
        if (ackSet.size() >= config.getDefaultPendingFrameSets() - 1) {
            sendResponses(ctx);
            ctx.flush();
        }
    }

    /**
     * Recalls the oldest outstanding frame sets after the adaptive acknowledgement deadline.
     * The insertion-ordered map permits the scan to stop at the first frame set that is still within its deadline.
     */
    protected void recallExpiredFrameSets() {
        final ObjectIterator<FrameSet> packetItr = pendingFrameSets.values().iterator();
        //2 sd from mean RTT is about 97% coverage
        final long timeout = retryTimeout();
        final long deadline = System.nanoTime() - timeout;
        boolean expired = false;
        while (packetItr.hasNext()) {
            final FrameSet frameSet = packetItr.next();
            if (frameSet.getSentTime() < deadline) {
                packetItr.remove();
                expired = true;
                recallFrameSet(frameSet);
            } else {
                break; // FrameSets are ordered by send time: LinkedMap
            }
        }
        if (expired) {
            acknowledgementTimeout = Math.min(MAX_RETRY_NANOS, Math.min(MAX_RETRY_NANOS, timeout) * 2);
        }
    }

    /**
     * Combines the ping estimate with bounded acknowledgement-driven backoff.
     *
     * @return positive retry wait in nanoseconds
     */
    private long retryTimeout() {
        long ping = config.getRTTNanos() + 2 * config.getRTTStdDevNanos() + config.getRetryDelayNanos();
        return Math.max(1, Math.max(ping, acknowledgementTimeout));
    }

    /**
     * Learns actual receipt delay while allowing prompt ACKs to reduce earlier backoff.
     *
     * @param frameSet outstanding datagram covered by this acknowledgement
     */
    private void acknowledgeTiming(final FrameSet frameSet) {
        long elapsed = Math.max(1, System.nanoTime() - frameSet.getSentTime());
        long observed = Math.min(MAX_RETRY_NANOS, elapsed) * 2;
        acknowledgementTimeout = Math.min(MAX_RETRY_NANOS,
                Math.max(observed, acknowledgementTimeout - acknowledgementTimeout / 8));
    }

    /**
     * Moves queued frame references into one MTU-budgeted frame set and retains a second reference for its write.
     * The original frame-set reference stays in the outstanding map until acknowledgement or recall.
     *
     * @param ctx connection used to queue the produced frame set
     * @param maxSize conservative byte budget for the complete frame set
     * @throws CorruptedFrameException if even the first queued frame cannot fit in the byte budget
     */
    protected void produceFrameSet(final ChannelHandlerContext ctx, final int maxSize) {
        if (frameQueue.isEmpty()) {
            return;
        }
        final FrameSet frameSet = FrameSet.create();
        Frame frame;
        while ((frame = frameQueue.peek()) != null) {
            assert frame.refCnt() > 0 : "Frame has lost reference";
            final int roughPacketSize = frame.getRoughPacketSize();
            if (frameSet.getRoughSize() + roughPacketSize > maxSize) {
                if (frameSet.isEmpty()) {
                    throw new CorruptedFrameException(
                            "Finished frame larger than the MTU by " + (roughPacketSize - maxSize));
                }
                break;
            }
            frameQueue.poll();
            this.queuedBytes -= roughPacketSize;
            frameSet.addPacket(frame);
        }
        if (!frameSet.isEmpty()) {
            frameSet.setSeqId(nextSendSeqId);
            nextSendSeqId = UINT.B3.plus(nextSendSeqId, 1);
            pendingFrameSets.put(frameSet.getSeqId(), frameSet);
            frameSet.touch("Added to pending FrameSet list");
            ctx.write(frameSet.retain()).addListener(RakNet.INTERNAL_WRITE_LISTENER);
            config.getMetrics().packetsOut(1);
            config.getMetrics().framesOut(frameSet.getNumPackets());
            config.getMetrics().currentQueuedBytes(this.queuedBytes);
            assert frameSet.refCnt() > 0;
        } else {
            frameSet.release();
        }
    }

    /**
     * Fills the baseline plus burst send window from queued frames using the configured MTU budget.
     *
     * @param ctx connection used for each produced frame-set write
     */
    protected void produceFrameSets(final ChannelHandlerContext ctx) {
        final int mtu = config.getMTU();
        final int maxSize = mtu - FrameSet.HEADER_SIZE - Frame.HEADER_SIZE;
        final int maxPendingFrameSets = config.getDefaultPendingFrameSets() + burstTokens;
        while (pendingFrameSets.size() < maxPendingFrameSets && !frameQueue.isEmpty()) {
            produceFrameSet(ctx, maxSize);
        }
    }

    /** Defers packet production on the installed executor so receipt handling does not reenter the send loop. */
    protected void tryProduceFrameSets() {
        ctx.executor().execute(frameSetProduction);
    }

    //TODO: instead of immediate recall, mark framesets as 'recalled', and flush at flush cycle
    /**
     * Requeues retained reliable frames for another transmission and completes unretransmitted unreliable writes.
     * The supplied frame-set reference is always released after its frames are examined.
     *
     * @param frameSet outstanding set whose ownership has been removed from the pending map
     */
    protected void recallFrameSet(final FrameSet frameSet) {
        try {
            adjustResendGauge(-1);
            config.getMetrics().bytesRecalled(frameSet.getRoughSize());
            frameSet.touch("Recalled");
            frameSet.createFrames(this::recallFrame);
            tryProduceFrameSets();
        } catch (CodecException failure) {
            frameSet.fail(failure);
            clearQueue(failure);
            throw failure;
        } finally {
            frameSet.release();
        }
    }

    /**
     * Transfers a retained retry reference to the queue or releases it on rejection.
     * Unreliable payloads are never retried and may have no application promise attached.
     *
     * @param frame retry reference distinct from the owning frame set's reference
     * @throws CodecException if the frame no longer fits the current MTU or queue budget
     */
    private void recallFrame(final Frame frame) {
        if (frame.getReliability().isReliable) {
            try {
                queueFrame(frame);
            } catch (CodecException failure) {
                frame.release();
                throw failure;
            }
        } else {
            final ChannelPromise promise = frame.getPromise();
            if (promise != null) {
                promise.trySuccess();
            }
            frame.release();
        }
    }

    /**
     * Applies high and low watermarks to unsent bytes and closes the connection if its hard queue limit is exceeded.
     * On a hard-limit failure, queued write promises are failed before all retained frames are released.
     *
     * @param ctx connection whose writable attribute and lifecycle are updated
     * @throws CodecException if queued bytes exceed the configured maximum
     */
    protected void updateBackPressure(final ChannelHandlerContext ctx) {
        final int queuedBytes = getQueuedBytes();
        final boolean oldWritable = ctx.channel().attr(RakNet.WRITABLE).get();
        boolean newWritable = oldWritable;
        if (queuedBytes > config.getMaxQueuedBytes()) {
            final CodecException t = new CodecException("Frame queue is too large");
            clearQueue(t);
            ctx.close();
            throw t;
        } else if (queuedBytes > config.getWriteBufferHighWaterMark()) {
            newWritable = false;
        } else if (queuedBytes < config.getWriteBufferLowWaterMark()) {
            newWritable = true;
        }
        if (newWritable != oldWritable) {
            ctx.channel().attr(RakNet.WRITABLE).set(newWritable ? Boolean.TRUE : Boolean.FALSE);
            ctx.fireChannelWritabilityChanged();
        }
    }

    /**
     * Returns the maintained estimate for unsent frames without rescanning their payloads.
     * Outstanding frame sets are accounted separately and are not included in this value.
     *
     * @return conservative queued frame bytes
     */
    protected int getQueuedBytes() {
//        int byteSize = 0;
//        for (Frame frame : frameQueue) {
//            byteSize += frame.getRoughPacketSize();
//        }
//        return byteSize;
        return this.queuedBytes;
    }

}
