package network.ycc.raknet.packet;

import java.util.List;
import network.ycc.raknet.utils.UINT;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import it.unimi.dsi.fastutil.ints.IntSortedSet;

/**
 * Encodes ACK or NACK sequence ranges without expanding them into individual datagram IDs.
 * Ranges use inclusive 24-bit endpoints and may wrap through zero.
 */
public class Reliability extends SimplePacket implements Packet {

    /** Shared empty range array used when constructing a reply from an empty ID set. */
    private static final REntry[] EMPTY = new REntry[0];
    /** Encoded ranges in packet order, initialized by decoding or a populated constructor. */
    private REntry[] entries;

    /** Leaves range storage unset for a decoder to populate before encoding. */
    private Reliability() {
    }

    /**
     * Creates one inclusive singleton range for the supplied datagram sequence.
     *
     * @param id 24-bit datagram sequence to acknowledge or recall
     */
    private Reliability(final int id) {
        entries = new REntry[]{new REntry(id)};
    }

    /**
     * Coalesces consecutive IDs in the supplied iteration order into inclusive ranges.
     * The input set is read but not retained or modified.
     *
     * @param ids sequence numbers sorted consistently around their active serial-number window
     */
    private Reliability(final IntSortedSet ids) {
        entries = EMPTY;
        if (ids.isEmpty()) {
            return;
        }
        List<REntry> res = new ArrayList<>();
        int startId = -1;
        int endId = -1;
        for (int i : ids) {
            if (startId == -1) {
                startId = i; //new region
                endId = i;
            } else if (i == UINT.B3.plus(endId, 1)) {
                endId = i; //continue region
            } else {
                res.add(new REntry(startId, endId));
                startId = i; //new region
                endId = i;
            }
        }
        res.add(new REntry(startId, endId));
        entries = res.toArray(entries);
    }

    /**
     * Writes a 16-bit range count followed by singleton flags and little-endian 24-bit endpoints.
     *
     * @param buf caller-owned destination positioned after the ACK or NACK packet ID
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeShort(entries.length);
        for (REntry entry : entries) {
            if (entry.idStart == entry.idFinish) {
                buf.writeBoolean(true);
                buf.writeMediumLE(entry.idStart);
            } else {
                buf.writeBoolean(false);
                buf.writeMediumLE(entry.idStart);
                buf.writeMediumLE(entry.idFinish);
            }
        }
    }

    /**
     * Reads the encoded range count and endpoints without expanding or interpreting their lengths.
     *
     * @param buf caller-owned source positioned after the ACK or NACK packet ID
     */
    @Override
    public void decode(final ByteBuf buf) {
        entries = new REntry[buf.readUnsignedShort()];
        for (int i = 0; i < entries.length; i++) {
            boolean single = buf.readBoolean();
            if (single) {
                entries[i] = new REntry(buf.readUnsignedMediumLE());
            } else {
                entries[i] = new REntry(buf.readUnsignedMediumLE(), buf.readUnsignedMediumLE());
            }
        }
    }

    /**
     * Copies the range table so replacing an element cannot change an acknowledgement already in use.
     * Range objects themselves have immutable endpoints and need no deep copy.
     *
     * @return independent range array, or null before a decoder-target instance is populated
     */
    public REntry[] getEntries() {
        return entries == null ? null : entries.clone();
    }

    /**
     * Returns the decoded range count without allocating a defensive copy on the receive path.
     *
     * @return number of ranges, or zero before decoding
     */
    public int getEntryCount() {
        return entries == null ? 0 : entries.length;
    }

    /**
     * Reads an immutable range directly without exposing the mutable table containing it.
     *
     * @param index zero-based position below {@link #getEntryCount()}
     * @return range whose endpoints cannot be changed by the caller
     * @throws IndexOutOfBoundsException if the position is outside the decoded range table
     */
    public REntry getEntry(final int index) {
        if (index < 0 || index >= getEntryCount()) {
            throw new IndexOutOfBoundsException("Acknowledgement range index: " + index);
        }
        return entries[index];
    }

    /** Stores one inclusive singleton or wrapping range of datagram sequence numbers. */
    public static class REntry {
        /** Inclusive first sequence number of this range. */
        public final int idStart;
        /** Inclusive last sequence number, possibly below the start when the range wraps. */
        public final int idFinish;

        /**
         * Creates a singleton range whose start and end are identical.
         *
         * @param id datagram sequence number
         */
        public REntry(final int id) {
            this(id, id);
        }

        /**
         * Stores range endpoints without normalization or range-length expansion.
         *
         * @param idstart inclusive first 24-bit sequence number
         * @param idfinish inclusive last 24-bit sequence number
         */
        public REntry(final int idstart, final int idfinish) {
            this.idStart = idstart;
            this.idFinish = idfinish;
        }
    }

    /** Confirms receipt of the datagrams covered by its inclusive ranges. */
    public static class ACK extends Reliability {
        /** Creates a decoder target whose ranges must be populated before encoding. */
        public ACK() {
        }

        /**
         * Creates an ACK covering exactly one datagram sequence.
         *
         * @param id datagram sequence covered by the reply
         */
        public ACK(final int id) {
            super(id);
        }

        /**
         * Creates an ACK by coalescing adjacent IDs in the supplied serial-number order.
         *
         * @param ids ordered datagram IDs read without retaining or modifying the set
         */
        public ACK(final IntSortedSet ids) {
            super(ids);
        }
    }

    /** Requests retransmission of outstanding datagrams covered by its inclusive ranges. */
    public static class NACK extends Reliability {
        /** Creates a decoder target whose ranges must be populated before encoding. */
        public NACK() {
        }

        /**
         * Creates a NACK covering exactly one datagram sequence.
         *
         * @param id datagram sequence covered by the reply
         */
        public NACK(final int id) {
            super(id);
        }

        /**
         * Creates a NACK by coalescing adjacent IDs in the supplied serial-number order.
         *
         * @param ids ordered datagram IDs read without retaining or modifying the set
         */
        public NACK(final IntSortedSet ids) {
            super(ids);
        }
    }

}
