package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.EncoderException;
import java.util.Arrays;
import network.ycc.raknet.config.DefaultConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Exercises high-bit wire lengths and rejects encodings that would truncate a sixteen-bit field. */
class UnsignedFieldsTest {
    /** UTF-8 byte counts, including the unsigned maximum, must round-trip independently of character count. */
    @Test
    void stringsRoundTripAtUnsignedBoundaries() {
        for (final String value : new String[] {text('x', 32767), text('x', 32768), text('x', 65535), text('\u20ac', 21845)}) {
            final ByteBuf wire = Unpooled.buffer();
            try {
                SimplePacket.writeString(wire, value);
                assertEquals(value, SimplePacket.readString(wire));
                assertEquals(0, wire.readableBytes());
            } finally {
                wire.release();
            }
        }
    }

    /** An oversized outbound string fails before it can corrupt a following packet's framing. */
    @Test
    void oversizedStringsDoNotWriteTruncatedLength() {
        final ByteBuf wire = Unpooled.buffer().writeByte(42);
        try {
            assertThrows(EncoderException.class, () -> SimplePacket.writeString(wire, text('x', 65536)));
            assertThrows(EncoderException.class, () -> SimplePacket.writeString(wire, text('\u20ac', 21846)));
            assertEquals(1, wire.writerIndex());
            assertEquals(42, wire.getByte(0));
        } finally {
            wire.release();
        }
    }

    /** A claimed string longer than the remaining packet cannot consume unrelated memory or yield partial text. */
    @Test
    void truncatedStringsAreRejected() {
        final ByteBuf wire = Unpooled.buffer().writeShort(65535).writeByte(1);
        try {
            assertThrows(IndexOutOfBoundsException.class, () -> SimplePacket.readString(wire));
        } finally {
            wire.release();
        }
    }

    /** Both handshake replies must interpret the negotiated MTU with the same unsigned semantics as the request. */
    @Test
    void replyMtuDoesNotBecomeNegative() {
        for (final AbstractConnectionReply reply : new AbstractConnectionReply[] {
                new ConnectionReply1(DefaultConfig.DEFAULT_MAGIC, 40000, 7),
                new ConnectionReply2(DefaultConfig.DEFAULT_MAGIC, 40000, 7, null)}) {
            final ByteBuf wire = Unpooled.buffer();
            try {
                reply.encode(wire);
                reply.setMtu(0);
                reply.decode(wire);
                assertEquals(40000, reply.getMtu());
            } finally {
                wire.release();
            }
        }
    }

    /** Neither reply may partially encode an MTU that would wrap to a different size on the wire. */
    @Test
    void unrepresentableMtuFailsBeforeWriting() {
        for (final AbstractConnectionReply reply : new AbstractConnectionReply[] {
                new ConnectionReply1(DefaultConfig.DEFAULT_MAGIC, -1, 7),
                new ConnectionReply2(DefaultConfig.DEFAULT_MAGIC, 65536, 7, null)}) {
            final ByteBuf wire = Unpooled.buffer();
            try {
                assertThrows(EncoderException.class, () -> reply.encode(wire));
                assertEquals(0, wire.writerIndex());
            } finally {
                wire.release();
            }
        }
    }

    /**
     * Builds fixed ASCII or multibyte fixtures on every supported Java generation.
     *
     * @param character repeated code point representable as one Java character
     * @param count number of characters, independent of encoded byte length
     * @return deterministic test string
     */
    private static String text(final char character, final int count) {
        final char[] characters = new char[count];
        Arrays.fill(characters, character);
        return new String(characters);
    }
}
