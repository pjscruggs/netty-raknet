package network.ycc.raknet.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

/** Exercises 24-bit modular arithmetic with reproducible samples spanning counter wraparound. */
public class UINTTests {
    /** Signed distance must recover an offset smaller than half the sequence space after wrapped addition. */
    @Test
    public void testMinusWrap() {
        Random random = new Random(0);
        for (int i = 0; i < 1000000; i++) {
            int a = (int) (random.nextDouble() * UINT.B3.MAX_VALUE);
            int d = (int) (random.nextDouble() * UINT.B3.MAX_VALUE) - UINT.B3.HALF_MAX;
            int b = UINT.B3.plus(a, d);

            Assertions.assertEquals(UINT.B3.minusWrap(b, a), d);
        }
    }

    /** Unsigned subtraction must differ from signed circular distance for at least one negative offset. */
    @Test
    public void testMinusFail() {
        Random random = new Random(0);
        boolean hasFailed = false;
        for (int i = 0; i < 1000000; i++) {
            int a = (int) (random.nextDouble() * UINT.B3.MAX_VALUE);
            int d = (int) (random.nextDouble() * UINT.B3.MAX_VALUE) - UINT.B3.HALF_MAX;
            int b = UINT.B3.plus(a, d);

            if (UINT.B3.minus(b, a) != d) {
                hasFailed = true;
            }
        }

        Assertions.assertTrue(hasFailed);
    }

    /** Checks adjacent iterator values against circular ordering, without claiming a global linear order. */
    @Test
    public void testCompareIterator() {
        Random random = new Random(0);
        IntSortedSet testSet = new IntRBTreeSet(UINT.B3.COMPARATOR);
        for (int i = 0; i < 500000; i++) {
            int a = (int) (random.nextDouble() * UINT.B3.MAX_VALUE);
            int b = (int) (random.nextDouble() * UINT.B3.MAX_VALUE);
            testSet.add(UINT.B3.plus(a, b));
        }
        int x = testSet.firstInt();
        IntBidirectionalIterator itr = testSet.iterator();
        while (itr.hasNext()) {
            int next = itr.nextInt();
            int d = UINT.B3.minusWrap(x, next);
            Assertions.assertTrue(d <= 0);
            x = next;
        }
    }
}
