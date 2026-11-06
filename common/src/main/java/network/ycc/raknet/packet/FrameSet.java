package network.ycc.raknet.packet;

import java.util.List;
import network.ycc.raknet.config.DefaultCodec;
import network.ycc.raknet.frame.Frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;

import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Owns the frames packed into one RakNet datagram and their pending write promises.
 * A live instance owns one reference to each stored frame. Releasing its last reference releases those
 * frames and returns this wrapper to a recycler. Mutable state belongs to the connection event loop.
 */
public final class FrameSet extends AbstractReferenceCounted implements Packet {

    /** Bytes occupied by the frame-set packet ID and little-endian 24-bit sequence number. */
    public static final int HEADER_SIZE = 4;

    /** Tracks frame-set references that never reach the final release. */
    private static final ResourceLeakDetector<FrameSet> leakDetector =
            ResourceLeakDetectorFactory.instance().newResourceLeakDetector(FrameSet.class);
    /** Reuses wrappers only after all owned frame references have been released. */
    private static final Recycler<FrameSet> recycler = new Recycler<FrameSet>() {
        /**
         * Creates an inactive wrapper that will be initialized when borrowed from the recycler.
         *
         * @param handle recycler handle permanently associated with the wrapper
         * @return wrapper with reference count zero
         */
        @Override
        protected FrameSet newObject(final Handle<FrameSet> handle) {
            return new FrameSet(handle);
        }
    };

    /** Frame references owned by this datagram, in wire order. */
    protected final List<Frame> frames = new ArrayList<>(8);
    /** Recycler handle that accepts this wrapper after deallocation. */
    protected final Recycler.Handle<FrameSet> handle;
    /** Datagram sequence number encoded in the low 24 bits of the header. */
    protected int seqId;
    /** Monotonic creation timestamp used as the resend-age baseline. */
    protected long sentTime;
    /** Leak-tracking record for the current borrowed lifetime, or null after release. */
    protected ResourceLeakTracker<FrameSet> tracker;
    /** Cached rough encoded size, including the datagram header. */
    protected int size;
    /**
     * Binds a new inactive wrapper to its recycler without claiming any frame references.
     *
     * @param handle recycler handle used after the final release
     */
    private FrameSet(final Recycler.Handle<FrameSet> handle) {
        this.handle = handle;
        setRefCnt(0);
    }

    /**
     * Borrows an empty frame set with one owned reference and a fresh monotonic timestamp.
     *
     * @return empty caller-owned frame set that must eventually be released
     */
    @SuppressWarnings("unchecked")
    public static FrameSet create() {
        final FrameSet out = recycler.get();
        assert out.refCnt() == 0;
        assert out.tracker == null;
        out.sentTime = System.nanoTime();
        out.seqId = 0;
        out.tracker = leakDetector.track(out);
        out.size = HEADER_SIZE;
        out.setRefCnt(1);
        return out;
    }

    /**
     * Consumes a datagram header and frames, retaining their payload slices.
     * The caller keeps its input-buffer reference. A partially parsed frame set is released on failure.
     *
     * @param buf caller-owned source including the recognized frame-set packet ID
     * @return new frame set owning one reference to each decoded frame
     * @throws CorruptedFrameException if the input ends inside a frame header or payload
     */
    public static FrameSet read(final ByteBuf buf) {
        final FrameSet out = create();
        try {
            out.size = buf.readableBytes();
            buf.skipBytes(1);
            out.seqId = buf.readUnsignedMediumLE();
            while (buf.isReadable()) {
                final Frame frame = Frame.read(buf);
                out.frames.add(frame);
            }
            return out.retain();
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptedFrameException("Failed to parse Frame", e);
        } finally {
            out.release();
        }
    }

    /**
     * Supplies the cached rough datagram size as the initial encoding allocation hint.
     *
     * @return header bytes plus cached frame-size estimates
     */
    @Override
    public int sizeHint() {
        return getRoughSize();
    }

    /**
     * Adds one shared owner to this live datagram without copying any frames.
     *
     * @return this frame set with its reference count incremented
     */
    @Override
    public FrameSet retain() {
        super.retain();
        return this;
    }

    /** Releases every owned frame, closes leak tracking and returns the empty wrapper to its recycler. */
    @Override
    protected void deallocate() {
        frames.forEach(Frame::release);
        frames.clear();
        size = HEADER_SIZE;
        if (tracker != null) {
            tracker.close(this);
            tracker = null;
        }
        handle.recycle(this);
    }

    /**
     * Creates a composite datagram using retained frame payloads rather than copying payload bytes.
     * The returned buffer remains valid independently of this frame set until its own reference is released.
     *
     * @param alloc allocator for the datagram header and composite wrapper
     * @return caller-owned encoded datagram buffer
     */
    public ByteBuf produce(final ByteBufAllocator alloc) {
        final ByteBuf header = alloc.ioBuffer(HEADER_SIZE, HEADER_SIZE);
        final CompositeByteBuf out = alloc.compositeDirectBuffer(1 + frames.size() * 2);
        try {
            writeHeader(header);
            out.addComponent(true, header.retain());
            frames.forEach(frame -> frame.produce(alloc, out));
            return out.retain();
        } finally {
            header.release();
            out.release();
        }
    }

    /**
     * Copies the header and frame bytes into an existing destination without transferring frame ownership.
     *
     * @param out caller-owned destination whose writer index advances
     */
    public void write(final ByteBuf out) {
        writeHeader(out);
        frames.forEach(frame -> frame.write(out));
    }

    /**
     * Appends the frame-set packet ID and low 24 bits of the datagram sequence in little-endian order.
     *
     * @param out caller-owned destination for the four-byte header
     */
    protected void writeHeader(final ByteBuf out) {
        out.writeByte(DefaultCodec.FRAME_DATA_START); //TODO: erm... ?
        out.writeMediumLE(seqId);
    }

    /**
     * Completes each attached frame promise successfully and clears it to prevent duplicate completion.
     * Frame references remain owned by this set until it is released.
     */
    public void succeed() {
        frames.forEach(frame -> {
            final ChannelPromise promise = frame.getPromise();
            if (promise != null) {
                promise.trySuccess();
                frame.setPromise(null);
            }
        });
    }

    /**
     * Fails and clears each attached frame promise without releasing the stored frames.
     *
     * @param e failure propagated to every outstanding frame promise
     */
    public void fail(final Throwable e) {
        frames.forEach(frame -> {
            final ChannelPromise promise = frame.getPromise();
            if (promise != null) {
                promise.tryFailure(e);
                frame.setPromise(null);
            }
        });
    }

    /**
     * Records an ownership hint for this wrapper and each owned frame without changing reference counts.
     *
     * @param hint diagnostic context retained by leak tracking when enabled
     * @return this frame set
     */
    @Override
    public ReferenceCounted touch(final Object hint) {
        if (tracker != null) {
            tracker.record(hint);
        }
        frames.forEach(packet -> packet.touch(hint));
        return this;
    }

    /**
     * Returns the monotonic timestamp captured when this frame set was borrowed.
     * Reliability handling uses it as the datagram's age baseline for retransmission.
     *
     * @return creation time in nanoseconds in the local clock domain
     */
    public long getSentTime() {
        return sentTime;
    }

    /**
     * Returns the datagram sequence used to match acknowledgements and retransmissions.
     *
     * @return assigned 24-bit sequence number
     */
    public int getSeqId() {
        return seqId;
    }

    /**
     * Assigns the sequence carried by this datagram without validating its width.
     *
     * @param seqId sequence number whose low 24 bits are encoded
     */
    public void setSeqId(final int seqId) {
        this.seqId = seqId;
    }

    /**
     * Counts the frame references currently owned by this datagram.
     *
     * @return number of frames in wire order
     */
    public int getNumPackets() {
        return frames.size();
    }

    /**
     * Transfers the supplied frame reference into this datagram without retaining it.
     * The caller must not release that transferred reference or add the same reference twice.
     *
     * @param packet frame reference whose ownership is transferred to this set
     */
    public void addPacket(final Frame packet) {
        frames.add(packet);
        size += packet.getRoughPacketSize();
    }

    /**
     * Passes one newly retained reference per frame to the consumer while preserving this set's references.
     * The consumer owns each delivered reference and must release or forward it, including on failure.
     *
     * @param consumer recipient accepting ownership of each additional frame reference
     */
    public void createFrames(final Consumer<Frame> consumer) {
        frames.forEach(frame -> consumer.accept(frame.retain()));
    }

    /**
     * Returns cached wire-size accounting used when packing frames under the MTU.
     * New sets accumulate conservative frame estimates, while decoded sets retain the original byte count.
     *
     * @return cached datagram size or size estimate in bytes
     */
    public int getRoughSize() {
//        int out = HEADER_SIZE;
//        for (Frame packet : frames) {
//            out += packet.getRoughPacketSize();
//        }
//        return out;
        return size;
    }

    /**
     * Tests whether this datagram owns any frames that could be transmitted.
     *
     * @return true when no frames have been added or decoded
     */
    public boolean isEmpty() {
        return frames.isEmpty();
    }

    /**
     * Reports frame count and sequence for diagnostics without traversing payload contents.
     *
     * @return compact frame-set description
     */
    @Override
    public String toString() {
        return String.format("FramedData(frames: %s, seq: %s)", frames.size(), getSeqId());
    }

}
