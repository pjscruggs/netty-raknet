package network.ycc.raknet.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Exercises counter normalization, initial sentinels, and signed overflow boundaries. */
class CounterBoundaryTest {
    /** Integer.MIN_VALUE must normalize instead of causing mutually recursive negation forever. */
    @Test
    void arbitrarySignedInputsNormalizeToWireWidth() {
        assertEquals(0, UINT.B3.minusWrap(Integer.MIN_VALUE, 0));
        assertEquals(0, UINT.B3.minusWrap(0, Integer.MIN_VALUE));
        assertEquals(7, UINT.B3.minusWrap(Integer.MIN_VALUE + 7, 0));
        assertEquals(-1, UINT.B3.minusWrap(Integer.MAX_VALUE, 0));
        assertEquals(1, UINT.B3.minusWrap(0, Integer.MAX_VALUE));
    }

    /** Initial minus-one counters and the ambiguous half-range tie retain their established signs. */
    @Test
    void initialSentinelsAndHalfRangeStayCompatible() {
        assertEquals(-1, UINT.B3.minusWrap(-1, 0));
        assertEquals(1, UINT.B3.minusWrap(0, -1));
        assertEquals(-0x800000, UINT.B3.minusWrap(0x800000, 0));
        assertEquals(0x800000, UINT.B3.minusWrap(0, 0x800000));
        assertEquals(-1, UINT.B3.minusWrap(0xffffff, 0));
        assertEquals(1, UINT.B3.minusWrap(0, 0xffffff));
    }

    /** Additions and subtractions remain modulo the wire width at signed integer extremes. */
    @Test
    void arithmeticDoesNotDependOnSignedOverflow() {
        assertEquals(0xfffffe, UINT.B3.plus(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertEquals(1, UINT.B3.minus(Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(0xfffe, UINT.B2.plus(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertEquals(1, UINT.B2.minus(Integer.MIN_VALUE, Integer.MAX_VALUE));
        assertEquals(0, UINT.B3.plus(0xffffff, 1));
        assertEquals(0xffffff, UINT.B3.minus(0, 1));
    }
}
