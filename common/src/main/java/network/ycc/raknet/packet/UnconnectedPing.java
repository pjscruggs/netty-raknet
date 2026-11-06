package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

/** Requests discovery information without creating a reliable RakNet session. */
public class UnconnectedPing extends SimplePacket implements Packet {

    /** Offline-message marker carried by this discovery request. */
    private RakNet.Magic magic;
    /** Opaque client clock value that the server must echo unchanged. */
    private long clientTime;
    /** Identifier announced by the discovering client. */
    private long clientId;

    /**
     * Appends client time, offline magic and client identity. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(clientTime);
        magic.write(buf);
        buf.writeLong(clientId);
    }

    /**
     * Reads the echoed-clock input, marker and client identity without creating session state. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
        clientTime = buf.readLong();
        magic = DefaultMagic.decode(buf);
        clientId = buf.readLong();
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
     * Exposes the discovery clock value for the server to echo without interpreting its units.
     *
     * @return opaque 64-bit value supplied by the discovering client
     */
    public long getClientTime() {
        return clientTime;
    }

    /**
     * Selects the discovery clock value that a matching pong should echo.
     *
     * @param clientTime timestamp or request marker meaningful in the client's clock domain
     */
    public void setClientTime(final long clientTime) {
        this.clientTime = clientTime;
    }

    /**
     * Exposes the client identity supplied with this request. The value alone does not authenticate the client.
     *
     * @return opaque 64-bit client identifier
     */
    public long getClientId() {
        return clientId;
    }

    /**
     * Stores the client identity to announce when this request is encoded.
     *
     * @param clientId opaque identifier matching the other messages in this connection attempt
     */
    public void setClientId(final long clientId) {
        this.clientId = clientId;
    }

}
