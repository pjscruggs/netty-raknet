package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;

/** Shares the negotiated MTU, server identity and offline marker between the two connection replies. */
public abstract class AbstractConnectionReply extends SimplePacket {

    /** The legacy RakNet security handshake is not advertised by these replies. */
    protected static final boolean NEEDS_SECURITY = false;

    /** Offline marker transmitted before the reply-specific negotiation fields. */
    protected RakNet.Magic magic;
    /** Negotiated maximum datagram size in bytes. */
    protected int mtu;
    /** Server identity returned in both offline handshake replies. */
    protected long serverId;

    /** Creates an empty reply whose fields must be populated before encoding. */
    public AbstractConnectionReply() {
    }

    /**
     * Stores negotiated reply settings without copying the magic implementation.
     *
     * @param magic offline marker shared with the server configuration
     * @param mtu negotiated maximum datagram size in bytes
     * @param serverId identity of the accepting server
     */
    public AbstractConnectionReply(final RakNet.Magic magic, final int mtu, final long serverId) {
        this.magic = magic;
        this.mtu = mtu;
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
     * Reports the server-selected datagram size. The concrete reply decoders currently read a signed short.
     *
     * @return stored MTU in bytes
     */
    public int getMtu() {
        return mtu;
    }

    /**
     * Stores the server-selected size without validating wire-width or transport bounds.
     *
     * @param mtu datagram size in bytes that must fit the receiving peer's MTU field
     */
    public void setMtu(final int mtu) {
        this.mtu = mtu;
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
