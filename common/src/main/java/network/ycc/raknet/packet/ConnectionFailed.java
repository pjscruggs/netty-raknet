package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

/** Signals that RakNet connection establishment failed, carrying an opaque peer-supplied code. */
public class ConnectionFailed extends SimplePacket implements Packet {

    /** Offline-message marker carried by the failure. */
    protected RakNet.Magic magic;
    /** Opaque 64-bit failure value, initially zero. */
    protected long code = 0;

    /** Creates an empty failure message for decoding. */
    public ConnectionFailed() {
    }

    /**
     * Creates a failure message with code zero and the supplied protocol marker.
     *
     * @param magic offline marker to encode
     */
    public ConnectionFailed(final RakNet.Magic magic) {
        this.magic = magic;
    }

    /**
     * Appends the offline marker followed by the failure value. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        buf.writeLong(code);
    }

    /**
     * Reads the marker and failure value without interpreting the code. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
        magic = DefaultMagic.decode(buf);
        code = buf.readLong();
    }

    /**
     * Exposes the offline marker carried by this packet without validating it against local configuration.
     *
     * @return stored marker, or null before initialization
     */
    public RakNet.Magic getMagic() {
        return magic;
    }

    /**
     * Selects the marker written by subsequent encodes, retaining the supplied object by reference.
     *
     * @param magic non-null offline marker whose contents remain stable while encoding
     */
    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    /**
     * Exposes the failure detail carried after the offline marker without interpreting its meaning.
     *
     * @return opaque 64-bit failure value
     */
    public long getCode() {
        return code;
    }

    /**
     * Stores the failure detail to write after the offline marker.
     *
     * @param code opaque value copied unchanged to the 64-bit wire field
     */
    public void setCode(final long code) {
        this.code = code;
    }

}
