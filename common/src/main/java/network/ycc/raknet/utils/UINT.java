package network.ycc.raknet.utils;

import it.unimi.dsi.fastutil.ints.IntComparator;

public class UINT {

    private UINT() {
    }

    public static class B2 {

        public static final int MAX_VALUE = ((1 << (Byte.SIZE * 2)) - 1);

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
            return ((value & MAX_VALUE) + (add & MAX_VALUE)) & MAX_VALUE;
        }

        /**
         * Subtracts the low sixteen bits while keeping the intermediate in the signed integer range.
         *
         * @param value current unsigned counter or its signed representation
         * @param minus decrement reduced modulo the wire counter width
         * @return wrapped unsigned sixteen-bit difference
         */
        public static int minus(final int value, final int minus) {
            return ((value & MAX_VALUE) - (minus & MAX_VALUE)) & MAX_VALUE;
        }

    }

    public static class B3 {

        public static final int MAX_VALUE = (1 << (Byte.SIZE * 3)) - 1;
        public static final int HALF_MAX = MAX_VALUE / 2;
        public static final IntComparator COMPARATOR = new Comparator();
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
            return ((value & MAX_VALUE) + (add & MAX_VALUE)) & MAX_VALUE;
        }

        /**
         * Subtracts normalized wire counters without relying on signed integer overflow.
         *
         * @param value current counter, normalized to its low twenty-four bits
         * @param minus subtracted counter, normalized to its low twenty-four bits
         * @return unsigned twenty-four-bit difference
         */
        public static int minus(final int value, final int minus) {
            return ((value & MAX_VALUE) - (minus & MAX_VALUE)) & MAX_VALUE;
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

        protected static class Comparator implements IntComparator {
            @Override
            public final int compare(final int a, final int b) {
                final int d = UINT.B3.minusWrap(a, b);
                return d < 0 ? -1 : (d == 0 ? 0 : 1);
            }
        }

    }

}
