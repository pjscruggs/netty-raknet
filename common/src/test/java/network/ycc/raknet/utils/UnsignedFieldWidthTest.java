package network.ycc.raknet.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks representability separately from modular arithmetic, which deliberately wraps counters. */
class UnsignedFieldWidthTest {
    /** Invalid widths cannot exploit Java's masked shift distance to appear representable. */
    @Test
    void rejectsInvalidWidthsAndOutOfRangeValues() {
        assertTrue(UINT.fits(65535, 16));
        assertTrue(UINT.fits(0, 16));
        assertTrue(UINT.fits(Integer.MAX_VALUE, 31));
        assertFalse(UINT.fits(65536, 16));
        assertFalse(UINT.fits(-1, 16));
        assertFalse(UINT.fits(Integer.MIN_VALUE, 31));
        assertFalse(UINT.fits(0, 0));
        assertFalse(UINT.fits(0, 32));
        assertFalse(UINT.fits(0, -1));
    }
}
