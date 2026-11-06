package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

/** Refuses a peer whose RakNet transport version is not accepted by the server. */
public class InvalidVersion extends SimplePacket implements Packet {

    /** Offline-message marker carried by the rejection. */
    private RakNet.Magic magic;
    /** Suggested transport version, encoded as one unsigned byte. */
    private int version;
    /** Identifier of the server rejecting the version. */
    private long serverId;
    /** Creates a decoder target with no marker and a zero suggested version. */
    public InvalidVersion() {

    }

    /**
     * Creates a version rejection with suggested version zero until explicitly changed.
     *
     * @param magic offline marker to encode
     * @param serverId identifier of the rejecting server
     */
    public InvalidVersion(final RakNet.Magic magic, final long serverId) {
        this.magic = magic;
        this.serverId = serverId;
    }

    /**
     * Appends the suggested version, offline marker and server identity. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeByte(version);
        magic.write(buf);
        buf.writeLong(serverId);
    }

    /**
     * Reads the suggested unsigned version, marker and server identity. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
        version = buf.readUnsignedByte();
        magic = DefaultMagic.decode(buf);
        serverId = buf.readLong();
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
     * Exposes the version advertised by the server in its rejection.
     *
     * @return stored transport version, decoded as an unsigned byte
     */
    public int getVersion() {
        return version;
    }

    /**
     * Selects the transport version advertised in this rejection without checking local support.
     *
     * @param version version whose low eight bits are encoded
     */
    public void setVersion(final int version) {
        this.version = version;
    }

    /**
     * Exposes the server identity advertised in this reply. The value alone does not authenticate the server.
     *
     * @return opaque 64-bit server identifier
     */
    public long getServerId() {
        return serverId;
    }

    /**
     * Stores the server identity to announce when this reply is encoded.
     *
     * @param serverId opaque identifier matching the responding server configuration
     */
    public void setServerId(final long serverId) {
        this.serverId = serverId;
    }

    /** Indicates that a peer rejected the offered RakNet transport version. */
    public static class InvalidVersionException extends DecoderException {
        /** Serialization identity retained for this decoder exception. */
        public static final long serialVersionUID = 590681756L;

        /** Creates a failure identifying a RakNet version mismatch without exposing packet contents. */
        public InvalidVersionException() {
            super("Incorrect RakNet version");
        }
    }

}
