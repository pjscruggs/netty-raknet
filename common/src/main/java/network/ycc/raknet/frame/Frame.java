package network.ycc.raknet.frame;

import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.UINT;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelPromise;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;

import java.util.List;

/**
 * Reference-counted RakNet frame metadata paired with retained packet or fragment bytes.
 * Counters and delivery metadata are assigned on the connection event loop before transmission.
 * A frame owns its FrameData reference and may also carry the write promise completed by reliable delivery.
 */
public final class Frame extends AbstractReferenceCounted {

    /** Scheduling comparator used to prioritize unreliable frames before reliable sequence order. */
    public static final FrameComparator COMPARATOR = new FrameComparator();
    /** Conservative maximum frame-header size used to reserve space before fragmentation. */
    public static final int HEADER_SIZE = 24;

    /** Wire flag indicating that split count, split identifier and fragment index follow the counter fields. */
    protected static final int SPLIT_FLAG = 0x10;

    /** Tracks each live frame acquisition independently of its recycled Java object. */
    private static final ResourceLeakDetector<Frame> leakDetector =
            ResourceLeakDetectorFactory.instance().newResourceLeakDetector(Frame.class);
    /** Reuses wrappers after payload ownership, promises and leak records have been cleared. */
    private static final Recycler<Frame> recycler = new Recycler<Frame>() {
        /**
         * Allocates an inactive wrapper whose counters and reference count are initialized on acquisition.
         *
         * @param handle recycler destination for the new wrapper
         * @return wrapper without a payload reference
         */
        @Override
        protected Frame newObject(final Handle<Frame> handle) {
            return new Frame(handle);
        }
    };

    /** Recycler handle retained for the lifetime of this reusable wrapper. */
    private final Recycler.Handle<Frame> handle;
    /** Whether the wire frame carries a fragment of a larger packet. */
    private boolean hasSplit;
    /** Twenty-four-bit reliable counter used for duplicate suppression and retransmission order. */
    private int reliableIndex;
    /** Twenty-four-bit freshness counter used by sequenced delivery modes. */
    private int sequenceIndex;
    /** Twenty-four-bit position within the payload's independent ordering channel. */
    private int orderIndex;
    /** Total number of fragments advertised for the packet identified by splitID. */
    private int splitCount;
    /** Sixteen-bit identifier associating fragments of the same outgoing packet. */
    private int splitID;
    /** Zero-based position of this fragment within its advertised fragment count. */
    private int splitIndex;
    /** Owned packet or fragment payload reference, cleared before wrapper recycling. */
    private FrameData frameData = null;
    /** Leak record for the active acquisition, or null when no record is active. */
    private ResourceLeakTracker<Frame> tracker = null;
    /** Optional originating write promise, shared by retransmission paths until delivery completes. */
    private ChannelPromise promise = null;

    /**
     * Creates a recycler-owned wrapper with no active reference or payload.
     *
     * @param handle destination used after the final release
     */
    private Frame(final Recycler.Handle<Frame> handle) {
        this.handle = handle;
        setRefCnt(0);
    }

    /**
     * Decodes one wire header and retains its payload slice while advancing the source reader index.
     * The source reference remains with the caller. Optional counters and split fields follow the decoded delivery mode.
     *
     * @param buf source positioned at the frame flags byte
     * @return decoded frame with one caller-owned reference
     */
    public static Frame read(final ByteBuf buf) {
        final Frame out = createRaw();
        try {
            final int flags = buf.readUnsignedByte();
            final int bitLength = buf.readUnsignedShort();
            final int length = (bitLength + Byte.SIZE - 1) / Byte.SIZE; //round up
            final boolean hasSplit = (flags & SPLIT_FLAG) != 0;
            final FramedPacket.Reliability reliability = FramedPacket.Reliability.get(flags >> 5);
            int orderChannel = 0;

            if (reliability.isReliable) {
                out.reliableIndex = buf.readUnsignedMediumLE();
            }
            if (reliability.isSequenced) {
                out.sequenceIndex = buf.readUnsignedMediumLE();
            }
            if (reliability.isOrdered) {
                out.orderIndex = buf.readUnsignedMediumLE();
                orderChannel = buf.readUnsignedByte();
            }
            if (hasSplit) {
                out.splitCount = buf.readInt();
                out.splitID = buf.readUnsignedShort();
                out.splitIndex = buf.readInt();
                out.hasSplit = true;
            }

            out.frameData = FrameData.read(buf, length, hasSplit);
            out.frameData.setReliability(reliability);
            out.frameData.setOrderChannel(orderChannel);

            return out.retain();
        } finally {
            out.release();
        }
    }

    /**
     * Wraps an unordered payload by retaining it without consuming the caller's original reference.
     * Ordered modes require explicitly assigned counters and must use createOrdered instead.
     *
     * @param packet unordered payload to retain
     * @return new unsplit frame with one caller-owned reference
     * @throws IllegalArgumentException if the payload requires ordering metadata
     */
    public static Frame create(final FrameData packet) {
        if (packet.getReliability().isOrdered) {
            throw new IllegalArgumentException("Must provided indices for ordered data.");
        }
        final Frame out = createRaw();
        out.frameData = packet.retain();
        return out;
    }

    /**
     * Retains an ordered payload and attaches the counters already selected for its ordering lane.
     * The caller keeps ownership of its original payload reference.
     *
     * @param packet ordered or sequenced payload to retain
     * @param orderIndex lane-local order counter assigned by the encoder
     * @param sequenceIndex freshness counter for sequenced modes, otherwise zero
     * @return new unsplit frame with one caller-owned reference
     * @throws IllegalArgumentException if the payload does not use ordering metadata
     */
    public static Frame createOrdered(final FrameData packet, final int orderIndex, final int sequenceIndex) {
        if (!packet.getReliability().isOrdered) {
            throw new IllegalArgumentException("No indices needed for non-ordered data.");
        }
        final Frame out = createRaw();
        out.frameData = packet.retain();
        out.orderIndex = orderIndex;
        out.sequenceIndex = sequenceIndex;
        return out;
    }

    /**
     * Acquires a clean frame wrapper with zeroed counters, no payload and one live reference.
     * The caller must install payload ownership before the frame can enter a pipeline.
     *
     * @return newly acquired wrapper
     */
    @SuppressWarnings("unchecked")
    private static Frame createRaw() {
        final Frame out = recycler.get();
        assert out.refCnt() == 0;
        assert out.tracker == null;
        assert out.frameData == null;
        assert out.promise == null;
        out.hasSplit = false;
        out.reliableIndex = out.sequenceIndex = out.orderIndex =
                out.splitCount = out.splitID = out.splitIndex = 0;
        out.setRefCnt(1);
        out.tracker = leakDetector.track(out);
        return out;
    }

    /**
     * Records the current owner on both the frame and its payload without retaining either again.
     *
     * @param hint context identifying the owner or pipeline stage
     * @return this frame with an unchanged reference count
     */
    @Override
    public ReferenceCounted touch(final Object hint) {
        if (tracker != null) {
            tracker.record(hint);
        }
        frameData.touch(hint);
        return this;
    }

    /**
     * Builds a complete frame from assembled bytes while preserving this sample fragment's delivery metadata.
     * The assembled buffer is retained as a slice and its reader index advances. Its original reference remains with the caller.
     *
     * @param fullData readable bytes of the complete packet, including its packet identifier
     * @return unsplit frame owning the assembled payload slice
     */
    public Frame completeFragment(final ByteBuf fullData) {
        assert frameData.isFragment();
        final Frame out = createRaw();
        out.reliableIndex = reliableIndex;
        out.sequenceIndex = sequenceIndex;
        out.orderIndex = orderIndex;
        out.frameData = FrameData.read(fullData, fullData.readableBytes(), false);
        out.frameData.setOrderChannel(getOrderChannel());
        out.frameData.setReliability(getReliability());
        return out;
    }

    /**
     * Appends independently retained fragments while preserving ordering counters and upgrading delivery to reliable.
     * Each fragment receives the next wrapped reliable counter. The original frame and payload indices remain unchanged.
     *
     * @param splitID identifier shared by every fragment of this packet
     * @param splitSize byte budget per fragment including the conservative header allowance, greater than HEADER_SIZE
     * @param reliableIndex reliable counter to assign to the first fragment
     * @param outList destination that takes ownership of each appended frame reference
     * @return number of fragments appended and reliable counters consumed
     * @throws IllegalStateException if a produced fragment exceeds the requested byte budget
     */
    public int fragment(final int splitID, final int splitSize, final int reliableIndex, final List<Object> outList) {
        final ByteBuf data = frameData.createData();
        int nextReliableIndex = reliableIndex;
        try {
            final int dataSplitSize = splitSize - HEADER_SIZE;
            final int splitCountTotal =
                    (data.readableBytes() + dataSplitSize - 1) / dataSplitSize; //round up
            for (int splitIndexIterator = 0; splitIndexIterator < splitCountTotal;
                    splitIndexIterator++) {
                final int length = Math.min(dataSplitSize, data.readableBytes());
                final Frame out = createRaw();
                out.reliableIndex = nextReliableIndex;
                out.sequenceIndex = sequenceIndex;
                out.orderIndex = orderIndex;
                out.splitCount = splitCountTotal;
                out.splitID = splitID;
                out.splitIndex = splitIndexIterator;
                out.hasSplit = true;
                out.frameData = FrameData.read(data, length, true);
                out.frameData.setOrderChannel(getOrderChannel());
                out.frameData.setReliability(getReliability().makeReliable()); //reliable form only
                assert out.frameData.isFragment();
                if (out.getRoughPacketSize() > splitSize) {
                    throw new IllegalStateException("mtu fragment mismatch");
                }
                nextReliableIndex = UINT.B3.plus(nextReliableIndex, 1);
                outList.add(out);
            }
            assert !data.isReadable();
            return splitCountTotal;
        } finally {
            data.release();
        }
    }

    /**
     * Returns an independently releasable view of fragment bytes without changing this frame's indices.
     * This operation is intended only for a fragment awaiting reassembly.
     *
     * @return retained duplicate whose reference must be released by the caller
     */
    public ByteBuf retainedFragmentData() {
        assert frameData.isFragment();
        return frameData.createData();
    }

    /**
     * Acquires another reference to the frame and extends its owned payload's lifetime.
     *
     * @return this frame for fluent ownership transfer
     */
    @Override
    public Frame retain() {
        return (Frame) super.retain();
    }

    /** Releases payload storage, clears the write promise and leak record, then recycles the frame wrapper. */
    @Override
    protected void deallocate() {
        if (frameData != null) {
            frameData.release();
            frameData = null;
        }
        if (tracker != null) {
            tracker.close(this);
            tracker = null;
        }
        promise = null;
        handle.recycle(this);
    }

    /**
     * Acquires a caller-owned reference to the same mutable payload wrapper rather than copying it.
     *
     * @return payload reference that the caller must release or transfer onward
     */
    public FrameData retainedFrameData() {
        return frameData.retain();
    }

    /**
     * Appends an encoded header and retained payload view to an existing composite without copying payload bytes.
     * The destination takes ownership of the appended components. This frame retains its own payload reference.
     *
     * @param alloc allocator for the header component
     * @param out composite whose writer index advances across the appended frame
     */
    public void produce(final ByteBufAllocator alloc, final CompositeByteBuf out) {
        final ByteBuf header = alloc.ioBuffer(HEADER_SIZE, HEADER_SIZE);
        try {
            writeHeader(header);
            out.addComponent(true, header.retain());
            out.addComponent(true, frameData.createData());
        } finally {
            header.release();
        }
    }

    /**
     * Copies the encoded header and payload to a destination while preserving the payload reader index.
     *
     * @param out buffer whose writer index advances by the complete encoded frame length
     */
    public void write(final ByteBuf out) {
        writeHeader(out);
        frameData.write(out);
    }

    /**
     * Encodes delivery flags, payload bit length and the counters required by the selected delivery mode.
     * Split fields follow the ordered metadata when the frame represents a fragment.
     *
     * @param out destination positioned where the frame header should begin
     */
    protected void writeHeader(final ByteBuf out) {
        out.writeByte((getReliability().code() << 5) | (hasSplit ? SPLIT_FLAG : 0));
        out.writeShort(frameData.getDataSize() * Byte.SIZE);

        assert !(hasSplit && !getReliability().isReliable);

        if (getReliability().isReliable) {
            out.writeMediumLE(reliableIndex);
        }
        if (getReliability().isSequenced) {
            out.writeMediumLE(sequenceIndex);
        }
        if (getReliability().isOrdered) {
            out.writeMediumLE(orderIndex);
            out.writeByte(getOrderChannel());
        }
        if (hasSplit) {
            out.writeInt(splitCount);
            out.writeShort(splitID);
            out.writeInt(splitIndex);
        }
    }

    /**
     * Returns the payload's current delivery mode used to select wire-header fields.
     *
     * @return reliability, ordering and sequencing policy
     */
    public FramedPacket.Reliability getReliability() {
        return frameData.getReliability();
    }

    /**
     * Returns the freshness counter used to discard older traffic in a sequenced lane.
     *
     * @return twenty-four-bit sequenced counter, meaningful only for sequenced delivery
     */
    public int getSequenceIndex() {
        return sequenceIndex;
    }

    /**
     * Returns the independent ordering lane stored with the payload.
     *
     * @return lane number used by ordered or sequenced delivery
     */
    public int getOrderChannel() {
        return frameData.getOrderChannel();
    }

    /**
     * Returns this packet's position within its ordering lane, shared by all of its fragments.
     *
     * @return twenty-four-bit order counter
     */
    public int getOrderIndex() {
        return orderIndex;
    }

    /**
     * Reports whether the frame carries split metadata and only part of a packet.
     *
     * @return true when reassembly is required before packet decoding
     */
    public boolean hasSplit() {
        return hasSplit;
    }

    /**
     * Returns the identifier grouping fragments of the same packet.
     *
     * @return sixteen-bit split identifier, meaningful only when hasSplit is true
     */
    public int getSplitId() {
        return splitID;
    }

    /**
     * Returns this fragment's advertised position within its split group.
     *
     * @return zero-based fragment index from the wire or splitter
     */
    public int getSplitIndex() {
        return splitIndex;
    }

    /**
     * Returns the advertised number of fragments required to complete this packet.
     *
     * @return split count from the wire or splitter, not validated by this accessor
     */
    public int getSplitCount() {
        return splitCount;
    }

    /**
     * Reports payload bytes without the RakNet frame header.
     * Complete packet payloads include their leading packet identifier.
     *
     * @return readable payload or fragment length
     */
    public int getDataSize() {
        return frameData.getDataSize();
    }

    /**
     * Adds the maximum header allowance to the payload length for conservative MTU and queue accounting.
     *
     * @return estimated frame size, which may exceed the actual encoded size
     */
    public int getRoughPacketSize() {
        return getDataSize() + HEADER_SIZE;
    }

    /**
     * Assigns the transport's reliable counter before the frame is queued for transmission.
     *
     * @param reliableIndex twenty-four-bit counter selected by the splitter
     */
    public void setReliableIndex(final int reliableIndex) {
        this.reliableIndex = reliableIndex;
    }

    /**
     * Returns the counter used for reliable ordering and duplicate identification.
     *
     * @return assigned reliable counter, meaningful only for reliable delivery
     */
    public int getReliableIndex() {
        return this.reliableIndex;
    }

    /**
     * Returns the originating write promise without completing it or changing payload ownership.
     *
     * @return write completion promise, or null when none is attached
     */
    public ChannelPromise getPromise() {
        return promise;
    }

    /**
     * Associates delivery completion with a write or clears that association after completion.
     * The frame does not complete a previously attached promise when it is replaced.
     *
     * @param promise originating write promise, or null to clear the association
     */
    public void setPromise(final ChannelPromise promise) {
        this.promise = promise;
    }

    /**
     * Prioritizes unreliable frames, then compares reliable frames using circular counter distance.
     * This scheduling comparison does not provide a total order for distinct unreliable frames or equal counters.
     */
    protected static final class FrameComparator implements java.util.Comparator<Frame> {
        /**
         * Places unreliable traffic first and orders reliable counters within the unambiguous half-counter range.
         * Only identical frame objects compare equal in this implementation.
         *
         * @param a first scheduled frame
         * @param b second scheduled frame
         * @return scheduling precedence for the frame queue
         */
        @Override
        public int compare(final Frame a, final Frame b) {
            if (a == b) {
                return 0;
            } else if (!a.getReliability().isReliable) {
                return -1;
            } else if (!b.getReliability().isReliable) {
                return 1;
            }
            return UINT.B3.minusWrap(a.reliableIndex, b.reliableIndex) < 0 ? -1 : 1;
        }
    }

}
