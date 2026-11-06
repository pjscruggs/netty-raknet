package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

/** Answers discovery with the echoed client clock, server identity and UTF-8 advertisement text. */
public class UnconnectedPong extends SimplePacket implements Packet {

    /** Client clock value echoed unchanged from the discovery request. */
    private long clientTime = 0L;
    /** Identifier of the advertising server. */
    private long serverId = 0L;
    /** Offline-message marker carried by this discovery reply. */
    private RakNet.Magic magic;
    /** Server advertisement text encoded as a length-prefixed UTF-8 string. */
    private String info = "";

    /** Creates a decoder target with zero identifiers and an empty advertisement. */
    public UnconnectedPong() {
    }

    /**
     * Creates a discovery reply using the request's clock value without interpreting that clock.
     *
     * @param clientTime clock value copied from the request
     * @param serverId identifier of the advertising server
     * @param magic offline marker to encode
     * @param info UTF-8 advertisement text
     */
    public UnconnectedPong(final long clientTime, final long serverId, final RakNet.Magic magic, final String info) {
        this.clientTime = clientTime;
        this.serverId = serverId;
        this.magic = magic;
        this.info = info;
    }

    /**
     * Appends echoed client time, server identity, offline magic and the advertisement. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(clientTime);
        buf.writeLong(serverId);
        magic.write(buf);
        writeString(buf, info);
    }

    /**
     * Reads echoed client time, server identity, magic and the length-prefixed advertisement. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     */
    @Override
    public void decode(final ByteBuf buf) {
        clientTime = buf.readLong();
        serverId = buf.readLong();
        magic = DefaultMagic.decode(buf);
        info = readString(buf);
    }

    /**
     * Exposes the discovery value echoed from the initiating ping.
     *
     * @return unchanged client clock value used to associate the reply with its request
     */
    public long getClientTime() {
        return clientTime;
    }

    /**
     * Copies the initiating discovery value into this reply without converting it to server time.
     *
     * @param clientTime value received in the corresponding unconnected ping
     */
    public void setClientTime(final long clientTime) {
        this.clientTime = clientTime;
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
     * Exposes the application advertisement returned during server discovery.
     *
     * @return advertised text, or null before initialization
     */
    public String getInfo() {
        return info;
    }

    /**
     * Stores the discovery advertisement for subsequent UTF-8 encoding.
     *
     * @param info non-null text whose UTF-8 byte count must fit the peer's string-length field
     */
    public void setInfo(final String info) {
        this.info = info;
    }

}
