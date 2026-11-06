package network.ycc.raknet.config;

import io.netty.buffer.ByteBuf;
import network.ycc.raknet.RakNet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks that marker comparison never leaves its temporary serialization buffer owned after return. */
class DefaultMagicOwnershipTest {
    /** Successful verification releases the buffer borrowed by the real marker reader. */
    @Test
    void releasesTemporaryBufferAfterSuccessfulComparison() {
        final TrackingMagic verifier = new TrackingMagic(new byte[16]);
        new DefaultMagic(new byte[16]).verify(verifier);
        assertNotNull(verifier.observed);
        assertEquals(0, verifier.observed.refCnt());
    }

    /** A mismatched marker still releases the partially consumed comparison buffer. */
    @Test
    void releasesTemporaryBufferAfterMarkerMismatch() {
        final byte[] expected = new byte[16];
        expected[0] = 1;
        final TrackingMagic verifier = new TrackingMagic(expected);
        assertThrows(RakNet.Magic.MagicMismatchException.class,
                () -> new DefaultMagic(new byte[16]).verify(verifier));
        assertNotNull(verifier.observed);
        assertEquals(0, verifier.observed.refCnt());
    }

    /** Failure while serializing a custom marker must not bypass the same cleanup path. */
    @Test
    void releasesTemporaryBufferWhenMarkerWriterThrows() {
        final FailingWriter writer = new FailingWriter();
        assertThrows(IllegalStateException.class, () -> writer.verify(new DefaultMagic(new byte[16])));
        assertNotNull(writer.observed);
        assertEquals(0, writer.observed.refCnt());
    }

    /** Captures the reader's borrowed buffer while retaining the real marker-comparison behavior. */
    private static final class TrackingMagic extends DefaultMagic {
        /** Borrowed reference used only to inspect ownership after verification has finished. */
        private ByteBuf observed;

        /**
         * Supplies the exact bytes that the real validator should accept.
         *
         * @param bytes expected marker bytes retained by the inherited implementation
         */
        private TrackingMagic(final byte[] bytes) {
            super(bytes);
        }

        /**
         * Records the borrowed buffer and delegates validation without retaining an extra reference.
         *
         * @param buf temporary comparison buffer owned by the verifying caller
         */
        @Override
        public void read(final ByteBuf buf) {
            observed = buf;
            super.read(buf);
        }
    }

    /** Exercises the failure path before another marker reader receives the temporary buffer. */
    private static final class FailingWriter extends DefaultMagic {
        /** Borrowed buffer captured before the writer throws, without an additional retain. */
        private ByteBuf observed;

        /** Supplies a standard-sized marker while replacing only its serialization behavior. */
        private FailingWriter() {
            super(new byte[16]);
        }

        /**
         * Captures the buffer and fails before comparison can run.
         *
         * @param buf temporary serialization buffer owned by the verifying caller
         * @throws IllegalStateException always, to exercise writer-failure cleanup
         */
        @Override
        public void write(final ByteBuf buf) {
            observed = buf;
            throw new IllegalStateException("Marker serialization failed");
        }
    }
}
