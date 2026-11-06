package network.ycc.raknet.pipeline;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Arrays;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.utils.UINT;

/** Intersects wire ranges with actual pending packets without expanding the 24-bit sequence space. */
final class AcknowledgementRanges {
    /** Sorted, disjoint inclusive intervals packed as a 24-bit start followed by a 24-bit end. */
    private final long[] intervals;
    /** Number of merged intervals occupying the prefix of {@link #intervals}. */
    private final int size;

    /**
     * Splits wrapping ranges and merges overlaps so repeated ranges cannot multiply pending work.
     *
     * @param entries inclusive sequence ranges decoded from one acknowledgement
     */
    private AcknowledgementRanges(final Reliability.REntry[] entries) {
        intervals = new long[Math.multiplyExact(entries.length, 2)];
        int count = 0;
        for (final Reliability.REntry entry : entries) {
            final int start = entry.idStart & UINT.B3.MAX_VALUE;
            final int end = entry.idFinish & UINT.B3.MAX_VALUE;
            if (start <= end) {
                intervals[count++] = pack(start, end);
            } else {
                intervals[count++] = pack(start, UINT.B3.MAX_VALUE);
                intervals[count++] = pack(0, end);
            }
        }
        Arrays.sort(intervals, 0, count);
        size = merge(count);
    }

    /**
     * Takes a stable snapshot before completion callbacks can enqueue or acknowledge other packets.
     * Work depends on the encoded entry count and pending packets, never the claimed range lengths.
     *
     * @param entries ranges supplied by the peer
     * @param pending sequence numbers that still own an outstanding frame set
     * @return matching pending numbers, each at most once
     */
    static IntArrayList select(final Reliability.REntry[] entries, final IntSet pending) {
        final IntArrayList selected = new IntArrayList();
        if (pending.isEmpty() || entries.length == 0) {
            return selected;
        }
        if (entries.length == 1 && entries[0].idStart == entries[0].idFinish) {
            final int sequence = entries[0].idStart & UINT.B3.MAX_VALUE;
            if (pending.contains(sequence)) {
                selected.add(sequence);
            }
            return selected;
        }
        final AcknowledgementRanges ranges = new AcknowledgementRanges(entries);
        for (final int sequence : pending) {
            if (ranges.contains(sequence)) {
                selected.add(sequence);
            }
        }
        return selected;
    }

    /**
     * Coalesces adjacent and overlapping sorted intervals in place.
     *
     * @param count populated interval count before merging
     * @return number of disjoint intervals retained
     */
    private int merge(final int count) {
        int merged = 0;
        for (int index = 0; index < count; index++) {
            final long current = intervals[index];
            if (merged > 0 && (current >>> 24) <= end(intervals[merged - 1]) + 1L) {
                final long previous = intervals[merged - 1];
                intervals[merged - 1] = pack((int) (previous >>> 24),
                        Math.max(end(previous), end(current)));
            } else {
                intervals[merged++] = current;
            }
        }
        return merged;
    }

    /**
     * Searches disjoint intervals without iterating the absent sequence numbers between them.
     *
     * @param sequence pending 24-bit sequence number
     * @return whether any peer range covers this outstanding packet
     */
    private boolean contains(final int sequence) {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            final int middle = (low + high) >>> 1;
            final long interval = intervals[middle];
            if (sequence < (interval >>> 24)) {
                high = middle - 1;
            } else if (sequence > end(interval)) {
                low = middle + 1;
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Encodes an inclusive interval in a positive long so natural sorting orders its start first.
     *
     * @param start normalized first sequence number
     * @param end normalized last sequence number
     * @return packed start and end
     */
    private static long pack(final int start, final int end) {
        return ((long) start << 24) | end;
    }

    /**
     * Extracts the inclusive upper endpoint of a packed interval.
     *
     * @param interval packed endpoints
     * @return normalized last sequence number
     */
    private static int end(final long interval) {
        return (int) (interval & UINT.B3.MAX_VALUE);
    }
}
