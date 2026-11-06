package network.ycc.raknet.config;

import network.ycc.raknet.RakNet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Reads, writes and compares the offline marker that identifies a RakNet datagram format.
 * The constructor retains its array directly, so callers must keep those bytes stable during use.
 */
public class DefaultMagic implements RakNet.Magic {

    //TODO: static cache?

    /** Shared marker bytes retained directly from the constructor argument. */
    protected final byte[] magicData;

    /**
     * Stores marker bytes by reference without copying or enforcing the standard sixteen-byte length.
     *
     * @param magicData marker array that must remain stable while this instance is used
     */
    public DefaultMagic(final byte[] magicData) {
        this.magicData = magicData;
    }

    /**
     * Copies the next sixteen bytes into a marker object without comparing them to local configuration.
     *
     * @param buf caller-owned source whose reader index advances by sixteen
     * @return marker owning its newly copied byte array
     */
    public static DefaultMagic decode(final ByteBuf buf) {
        final byte[] magicData = new byte[16];
        buf.readBytes(magicData);
        return new DefaultMagic(magicData);
    }

    /**
     * Appends the stored marker bytes without changing or releasing the destination's existing content.
     *
     * @param buf caller-owned destination whose writer index advances by the marker length
     */
    @Override
    public void write(final ByteBuf buf) {
        buf.writeBytes(magicData);
    }

    /**
     * Consumes and compares the stored marker one byte at a time.
     * On a mismatch, the reader index remains after the mismatching byte rather than rolling back.
     *
     * @param buf caller-owned source positioned at the candidate marker
     * @throws MagicMismatchException if any consumed byte differs from the configured marker
     */
    @Override
    public void read(final ByteBuf buf) {
        for (byte b : magicData) {
            if (buf.readByte() != b) {
                throw new MagicMismatchException();
            }
        }
    }

    /**
     * Compares this marker with another marker implementation through its validation method.
     * The temporary serialization buffer is released after validation succeeds or throws.
     *
     * @param other marker implementation whose read method validates the serialized bytes
     */
    @Override
    public void verify(final RakNet.Magic other) {
        final ByteBuf tmp = Unpooled.buffer(16);
        try {
            write(tmp);
            other.read(tmp);
        } finally {
            tmp.release();
        }
    }

}
