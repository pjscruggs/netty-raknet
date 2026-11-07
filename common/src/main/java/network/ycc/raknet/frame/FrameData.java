package network.ycc.raknet.frame;

import network.ycc.raknet.packet.FramedPacket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;

/**
 * Reference-counted packet bytes and delivery metadata shared by the framing pipeline.
 * The payload is retained separately from the Java wrapper, which is recycled after its last release.
 * Metadata and buffer indices are mutable and belong to the owning connection event loop.
 */
public final class FrameData extends AbstractReferenceCounted implements FramedPacket {

    /** Tracks wrapper lifetimes so a lost frame reference can be reported with its access hints. */
    private static final ResourceLeakDetector<FrameData> leakDetector =
            ResourceLeakDetectorFactory.instance().newResourceLeakDetector(FrameData.class);
    /** Reuses wrappers only after their payload reference and leak tracker have been cleared. */
    private static final Recycler<FrameData> recycler = new Recycler<FrameData>() {
        /**
         * Allocates an inactive wrapper for the recycler. The acquisition path initializes its reference count.
         *
         * @param handle recycler handle used when the wrapper is released
         * @return new wrapper with no payload ownership
         */
        @Override
        protected FrameData newObject(final Handle<FrameData> handle) {
            return new FrameData(handle);
        }
    };
    /** Returns this wrapper to its originating recycler after final release. */
    private final Recycler.Handle<FrameData> handle;
    /** Leak record for the current acquisition, or null when the wrapper is inactive. */
    private ResourceLeakTracker<FrameData> tracker;
    /** Ordering channel carried with the payload, used only by delivery modes with ordering metadata. */
    private int orderId;
    /** Whether these bytes are only part of a packet and therefore lack a usable leading packet identifier. */
    private boolean fragment;
    /** Owned retained payload view, including the packet identifier for complete packets. */
    private ByteBuf data;
    /** Delivery policy selected before framing assigns reliable, ordered and sequenced counters. */
    private FramedPacket.Reliability reliability;
    /**
     * Creates a recyclable wrapper without acquiring a payload or a live caller reference.
     *
     * @param handle recycler destination for this wrapper
     */
    private FrameData(final Recycler.Handle<FrameData> handle) {
        this.handle = handle;
        setRefCnt(0);
    }

    /**
     * Acquires a clean wrapper with one reference and the default reliable ordered delivery policy.
     * The caller must attach payload storage before exposing the wrapper to handlers.
     *
     * @return initialized wrapper owned by the caller
     */
    @SuppressWarnings("unchecked")
    private static FrameData createRaw() {
        final FrameData out = recycler.get();
        assert out.refCnt() == 0 && out.tracker == null : "bad reuse";
        out.orderId = 0;
        out.fragment = false;
        out.data = null;
        out.reliability = FramedPacket.Reliability.RELIABLE_ORDERED;
        out.setRefCnt(1);
        out.tracker = leakDetector.track(out);
        return out;
    }

    /**
     * Prepends a packet identifier to a retained view of the supplied payload without copying its bytes.
     * The caller keeps its original buffer reference and must release it separately. Source indices are unchanged.
     *
     * @param alloc allocator for the identifier byte and composite storage
     * @param packetId unsigned byte identifying the application or transport packet
     * @param buf readable payload whose storage remains shared with the returned frame
     * @return complete packet with one caller-owned reference
     */
    public static FrameData create(final ByteBufAllocator alloc, final int packetId, final ByteBuf buf) {
        final CompositeByteBuf out = alloc.compositeDirectBuffer(2);
        try {
            out.addComponent(true, alloc.ioBuffer(1, 1).writeByte(packetId));
            out.addComponent(true, buf.retain());
            return FrameData.read(out, out.readableBytes(), false);
        } finally {
            out.release();
        }
    }

    /**
     * Retains the next payload slice and advances the source reader index by its length.
     * The source buffer reference remains owned by the caller. Fragment slices are not interpreted as complete packets.
     *
     * @param buf source containing the payload bytes
     * @param length positive number of readable bytes to retain
     * @param fragment whether the slice is a fragment awaiting reassembly
     * @return payload wrapper with one caller-owned reference
     * @throws IllegalArgumentException if the payload has no packet or fragment bytes
     */
    public static FrameData read(final ByteBuf buf, final int length, final boolean fragment) {
        if (length <= 0) {
            throw new IllegalArgumentException("Frame payload length must be positive");
        }
        final FrameData packet = createRaw();
        try {
            packet.data = buf.readRetainedSlice(length);
            packet.fragment = fragment;
            assert packet.getDataSize() == length;
            return packet.retain();
        } finally {
            packet.release();
        }
    }

    /**
     * Copies all readable payload bytes to the destination without moving this frame's reader index.
     * Neither buffer reference is released or transferred by this operation.
     *
     * @param out destination whose writer index advances by the payload length
     */
    public void write(final ByteBuf out) {
        out.writeBytes(data, data.readerIndex(), data.readableBytes());
    }

    /**
     * Returns a retained duplicate with independent indices and shared payload storage.
     * Releasing the returned view is the caller's responsibility, even when this frame is also released.
     *
     * @return independently releasable view of the current readable payload
     */
    public ByteBuf createData() {
        return data.retainedDuplicate();
    }

    /**
     * Acquires another ownership reference to this wrapper and its existing payload lifetime.
     *
     * @return this frame for fluent ownership transfer
     */
    @Override
    public FrameData retain() {
        return (FrameData) super.retain();
    }

    /** Releases the owned payload, closes its leak record and recycles the wrapper after the last reference. */
    @Override
    protected void deallocate() {
        if (data != null) {
            data.release();
            data = null;
        }
        if (tracker != null) {
            tracker.close(this);
            tracker = null;
        }
        handle.recycle(this);
    }

    /**
     * Describes delivery metadata and payload length without decoding fragment bytes as a packet identifier.
     *
     * @return diagnostic description valid while the frame is retained
     */
    @Override
    public String toString() {
        return String.format("PacketData(%s, length: %s, framed: %s, packetId: %s)",
                getReliability(), getDataSize(), isFragment(),
                fragment ? "n/a" : String.format("%02x", getPacketId()));
    }

    /**
     * Records an ownership hint on both the wrapper and its payload for leak diagnostics.
     *
     * @param hint context describing the current owner or pipeline stage
     * @return this frame without changing its reference count
     */
    @Override
    public ReferenceCounted touch(final Object hint) {
        if (tracker != null) {
            tracker.record(hint);
        }
        data.touch(hint);
        return this;
    }

    /**
     * Reads the unsigned leading identifier of a complete packet without consuming it.
     * Callers must not use this operation on a fragment awaiting reassembly.
     *
     * @return packet identifier in the range zero through 255
     */
    public int getPacketId() {
        assert !fragment;
        return data.getUnsignedByte(data.readerIndex());
    }

    /**
     * Reports the readable payload size, including the identifier byte for complete packets.
     *
     * @return bytes available from the owned payload view
     */
    public int getDataSize() {
        return data.readableBytes();
    }

    /**
     * Returns the delivery policy used when the framing pipeline assigns transport counters.
     *
     * @return current reliability, ordering and sequencing mode
     */
    @Override
    public FramedPacket.Reliability getReliability() {
        return reliability;
    }

    /**
     * Distinguishes partial packet bytes from a complete packet beginning with an identifier.
     *
     * @return true when packet interpretation must wait for reassembly
     */
    public boolean isFragment() {
        return fragment;
    }

    /**
     * Selects the delivery policy before this payload is framed or queued for transmission.
     * Changing shared metadata after handing a frame to another handler can invalidate its assigned counters.
     *
     * @param reliability non-null delivery mode for subsequent framing
     */
    @Override
    public void setReliability(final FramedPacket.Reliability reliability) {
        this.reliability = reliability;
    }

    /**
     * Returns the independent ordering lane associated with this payload.
     * Unordered delivery modes do not encode or consume this value.
     *
     * @return ordering channel selected by the application
     */
    @Override
    public int getOrderChannel() {
        return orderId;
    }

    /**
     * Selects the ordering lane before the payload enters the framing pipeline.
     * The transport supports channels zero through seven. This setter does not validate the supplied value.
     *
     * @param orderChannel ordering channel to use for ordered or sequenced delivery
     */
    @Override
    public void setOrderChannel(final int orderChannel) {
        this.orderId = orderChannel;
    }





}
