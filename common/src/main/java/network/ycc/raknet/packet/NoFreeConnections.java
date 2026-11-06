package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

/** Reports that the server has reached its configured connection capacity. */
public class NoFreeConnections extends SimplePacket implements Packet {

    /** Offline-message marker carried by this reply. */
    private RakNet.Magic magic;
    /** Identifier of the server sending the refusal. */
    private long serverId;

    /** Creates an uninitialized reply for the decoder to populate. */
    public NoFreeConnections() {

    }

    /**
     * Creates a refusal identifying the server and its offline protocol marker.
     *
     * @param magic offline marker to encode
     * @param serverId identifier of the refusing server
     */
    public NoFreeConnections(final RakNet.Magic magic, final long serverId) {
        this.magic = magic;
        this.serverId = serverId;
    }

    /**
     * Appends the offline marker and 64-bit server identifier. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        buf.writeLong(serverId);
    }

    /**
     * Reads the offline marker and server identifier without checking the marker against local configuration. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
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

}
