package network.ycc.raknet.pipeline;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import java.util.Arrays;
import java.util.Random;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.utils.UINT;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks sparse intersections, counter wrap and repeated hostile ranges against an independent oracle. */
class AcknowledgementRangesTest {
    /** A maximum wire entry count covering the entire counter space still selects only pending IDs. */
    @Test
    void repeatedFullRangesDoNotExpandSequenceSpace() {
        final Reliability.REntry[] entries = new Reliability.REntry[65535];
        Arrays.fill(entries, new Reliability.REntry(0, UINT.B3.MAX_VALUE));
        final int[] ids = {0, 7, 8000, UINT.B3.MAX_VALUE};
        final IntLinkedOpenHashSet pending = new IntLinkedOpenHashSet(ids);
        assertEquals(new IntArrayList(ids), AcknowledgementRanges.select(entries, pending));
        assertEquals(4, pending.size());
    }

    /** Adjacent and wrapping overlaps merge without dropping either endpoint or adding a missing ID. */
    @Test
    void wrappingOverlapsAndGapsPreserveExactMembership() {
        final Reliability.REntry[] entries = {
                new Reliability.REntry(UINT.B3.MAX_VALUE - 1, 2),
                new Reliability.REntry(2, 4), new Reliability.REntry(7),
                new Reliability.REntry(3, 5)};
        final IntLinkedOpenHashSet pending = new IntLinkedOpenHashSet(
                new int[] {6, 7, 0, 2, 3, 5, UINT.B3.MAX_VALUE - 1, UINT.B3.MAX_VALUE, 8});
        assertEquals(new IntArrayList(new int[] {7, 0, 2, 3, 5,
                UINT.B3.MAX_VALUE - 1, UINT.B3.MAX_VALUE}),
                AcknowledgementRanges.select(entries, pending));
    }

    /** The single-ID fast path and empty inputs preserve the same selection contract as intervals. */
    @Test
    void singleAndEmptyAcknowledgementsSelectOnlyOutstandingIds() {
        final IntLinkedOpenHashSet pending = new IntLinkedOpenHashSet(new int[] {7});
        assertEquals(new IntArrayList(new int[] {7}), AcknowledgementRanges.select(
                new Reliability.REntry[] {new Reliability.REntry(7)}, pending));
        assertTrue(AcknowledgementRanges.select(
                new Reliability.REntry[] {new Reliability.REntry(8)}, pending).isEmpty());
        assertTrue(AcknowledgementRanges.select(new Reliability.REntry[0], pending).isEmpty());
        assertTrue(AcknowledgementRanges.select(new Reliability.REntry[] {
                new Reliability.REntry(0, UINT.B3.MAX_VALUE)}, new IntLinkedOpenHashSet()).isEmpty());
    }

    /** Fixed random ranges are checked with modular distances rather than the production sort/search. */
    @Test
    void randomIntersectionsMatchModularDistanceOracle() {
        final Random random = new Random(0x52414b4e4554L);
        for (int trial = 0; trial < 1000; trial++) {
            final Reliability.REntry[] entries = new Reliability.REntry[1 + random.nextInt(16)];
            final IntLinkedOpenHashSet pending = new IntLinkedOpenHashSet();
            for (int index = 0; index < entries.length; index++) {
                final int start = random.nextInt(UINT.B3.MAX_VALUE + 1);
                final int end = random.nextInt(UINT.B3.MAX_VALUE + 1);
                entries[index] = new Reliability.REntry(start, end);
                pending.add(start);
                pending.add(end);
                pending.add(random.nextInt(UINT.B3.MAX_VALUE + 1));
            }
            assertEquals(expected(entries, pending), AcknowledgementRanges.select(entries, pending));
        }
    }

    /**
     * Computes membership directly from circular distance for a small fixture set.
     *
     * @param entries possibly overlapping inclusive ranges
     * @param pending outstanding fixture IDs in insertion order
     * @return reference intersection with no duplicates
     */
    private static IntArrayList expected(final Reliability.REntry[] entries,
            final IntLinkedOpenHashSet pending) {
        final IntArrayList expected = new IntArrayList();
        for (final int id : pending) {
            for (final Reliability.REntry entry : entries) {
                if (((id - entry.idStart) & UINT.B3.MAX_VALUE)
                        <= ((entry.idFinish - entry.idStart) & UINT.B3.MAX_VALUE)) {
                    expected.add(id);
                    break;
                }
            }
        }
        return expected;
    }
}
