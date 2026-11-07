package network.ycc.raknet.utils;

import it.unimi.dsi.fastutil.ints.IntComparator;

/** Provides modular arithmetic for the unsigned counters used in RakNet headers. */
public class UINT {

    /** Prevents construction of the static wire-counter helpers. */
    private UINT() {
    }

    /**
     * Checks whether a nonnegative integer fits a wire field without truncation or Java shift-width wrap.
     *
     * @param value integer about to be written to the wire
     * @param bits field width from one through thirty-one bits
     * @return whether the width is supported and every value bit fits within it
     */
    public static boolean fits(final int value, final int bits) {
        return bits > 0 && bits < Integer.SIZE && value >= 0 && (value >>> bits) == 0;
    }

    /**
     * Adds normalized operands for the supported widths without signed intermediate overflow.
     *
     * @param value counter to advance
     * @param increment amount to add, reduced to the selected width
     * @param mask contiguous low-bit mask for a 16-bit or 24-bit wire field
     * @return sum wrapped to the selected field width
     */
    private static int addMasked(final int value, final int increment, final int mask) {
        return ((value & mask) + (increment & mask)) & mask;
    }

    /**
     * Subtracts normalized operands using the same width rules for both wire-counter types.
     *
     * @param value counter to reduce
     * @param decrement amount to subtract, reduced to the selected width
     * @param mask contiguous low-bit mask for a 16-bit or 24-bit wire field
     * @return difference wrapped to the selected field width
     */
    private static int subtractMasked(final int value, final int decrement, final int mask) {
        return ((value & mask) - (decrement & mask)) & mask;
    }

    /** Normalizes and wraps arithmetic in the unsigned 16-bit sequence space. */
    public static class B2 {

        /** Mask selecting the low sixteen bits and the largest encoded 16-bit value. */
        public static final int MAX_VALUE = ((1 << (Byte.SIZE * 2)) - 1);

        /** Prevents construction of the static 16-bit arithmetic helper. */
        private B2() {
        }

        /**
         * Adds the low sixteen bits without overflowing the signed intermediate.
         *
         * @param value current unsigned counter or its signed representation
         * @param add increment reduced modulo the wire counter width
         * @return wrapped unsigned sixteen-bit sum
         */
        public static int plus(final int value, final int add) {
            return addMasked(value, add, MAX_VALUE);
        }

        /**
         * Subtracts the low sixteen bits while keeping the intermediate in the signed integer range.
         *
         * @param value current unsigned counter or its signed representation
         * @param minus decrement reduced modulo the wire counter width
         * @return wrapped unsigned sixteen-bit difference
         */
        public static int minus(final int value, final int minus) {
            return subtractMasked(value, minus, MAX_VALUE);
        }

    }

    /** Normalizes 24-bit datagram counters and compares positions within a bounded serial-number window. */
    public static class B3 {

        /** Mask selecting the low twenty-four bits and the largest encoded 24-bit value. */
        public static final int MAX_VALUE = (1 << (Byte.SIZE * 3)) - 1;
        /** Largest strictly shorter-than-half-cycle positive distance used to decide serial ordering. */
        public static final int HALF_MAX = MAX_VALUE / 2;
        /** Circular-order comparator for IDs confined to one active window shorter than half a cycle. */
        public static final IntComparator COMPARATOR = new Comparator();
        /** Prevents construction of the static 24-bit arithmetic helper. */
        private B3() {
        }

        /**
         * Adds normalized wire counters without relying on signed integer overflow.
         *
         * @param value current counter, normalized to its low twenty-four bits
         * @param add increment, normalized to its low twenty-four bits
         * @return unsigned twenty-four-bit sum
         */
        public static int plus(final int value, final int add) {
            return addMasked(value, add, MAX_VALUE);
        }

        /**
         * Subtracts normalized wire counters without relying on signed integer overflow.
         *
         * @param value current counter, normalized to its low twenty-four bits
         * @param minus subtracted counter, normalized to its low twenty-four bits
         * @return unsigned twenty-four-bit difference
         */
        public static int minus(final int value, final int minus) {
            return subtractMasked(value, minus, MAX_VALUE);
        }

        /**
         * Returns the shortest signed wire-counter distance without recursion or overflow.
         * The exact half-range tie preserves the existing opposite-sign ordering convention.
         *
         * @param value received or local counter, normalized to twenty-four bits
         * @param minus comparison counter, including the initial minus-one sentinel
         * @return signed distance within half of the circular sequence space
         */
        public static int minusWrap(final int value, final int minus) {
            final int dist = (value & MAX_VALUE) - (minus & MAX_VALUE);
            if (dist > HALF_MAX) {
                return dist - (MAX_VALUE + 1);
            }
            if (dist < -HALF_MAX) {
                return dist + MAX_VALUE + 1;
            }
            return dist;
        }

        /**
         * Orders serial numbers by their shortest circular distance, not by unsigned numeric magnitude.
         * Use only for an active window shorter than half the sequence space to retain transitive ordering.
         */
        protected static class Comparator implements IntComparator {
            /**
             * Compares two counters by the sign of their shortest wrapped difference.
             *
             * @param a first 24-bit counter
             * @param b second 24-bit counter
             * @return negative, zero or positive according to circular serial order
             */
            @Override
            public final int compare(final int a, final int b) {
                final int d = UINT.B3.minusWrap(a, b);
                return d < 0 ? -1 : (d == 0 ? 0 : 1);
            }
        }

    }

}
