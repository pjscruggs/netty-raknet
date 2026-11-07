package network.ycc.raknet.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks payload boundaries without relying on JVM assertions being enabled in production. */
class FrameDataValidationTest {
    /** Empty and negative payload sizes are runtime errors that leave the caller's buffer untouched. */
    @Test
    void invalidLengthsDoNotRetainOrConsumeInput() {
        final ByteBuf source = Unpooled.buffer().writeByte(42);
        try {
            assertThrows(IllegalArgumentException.class, () -> FrameData.read(source, 0, false));
            assertThrows(IllegalArgumentException.class, () -> FrameData.read(source, -1, true));
            assertEquals(1, source.refCnt());
            assertEquals(0, source.readerIndex());
            final FrameData valid = FrameData.read(source, 1, false);
            valid.release();
            assertEquals(1, source.refCnt());
        } finally {
            source.release();
        }
    }
}
