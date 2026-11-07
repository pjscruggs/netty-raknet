package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks that inspection cannot rewrite ACK contents and indexed reads need no range-table copy. */
class ReliabilityOwnershipTest {
    /** Replacing an exported array element must leave both processing and wire encoding unchanged. */
    @Test
    void exportedArrayCannotChangeAcknowledgedSequence() {
        final Reliability.ACK acknowledgement = new Reliability.ACK(7);
        acknowledgement.getEntries()[0] = new Reliability.REntry(42);
        assertEquals(7, acknowledgement.getEntry(0).idStart);
        assertSame(acknowledgement.getEntry(0), acknowledgement.getEntry(0));
        final ByteBuf wire = Unpooled.buffer();
        try {
            acknowledgement.encode(wire);
            final Reliability.ACK decoded = new Reliability.ACK();
            decoded.decode(wire);
            assertEquals(1, decoded.getEntryCount());
            assertEquals(7, decoded.getEntry(0).idStart);
            assertEquals(7, decoded.getEntry(0).idFinish);
        } finally {
            wire.release();
        }
    }

    /** Empty decoder targets and invalid indices have an explicit, consistent inspection contract. */
    @Test
    void undecodedAndOutOfRangeReadsAreChecked() {
        final Reliability.ACK empty = new Reliability.ACK();
        assertNull(empty.getEntries());
        assertEquals(0, empty.getEntryCount());
        assertThrows(IndexOutOfBoundsException.class, () -> empty.getEntry(0));
        final Reliability.ACK populated = new Reliability.ACK(7);
        assertThrows(IndexOutOfBoundsException.class, () -> populated.getEntry(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> populated.getEntry(1));
    }
}
