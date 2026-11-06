package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

/** Probes the path MTU and announces the client's RakNet protocol version before a session exists. */
public class ConnectionRequest1 extends SimplePacket implements Packet {

    /** Offline-message marker identifying the probe format. */
    private RakNet.Magic magic;
    /** RakNet transport version encoded in one byte. */
    private int protocolVersion;
    /** Probe size in bytes, inferred from the remaining payload during decoding. */
    private int mtu;

    /** Creates an uninitialized MTU probe for decoding. */
    public ConnectionRequest1() {
    }

    /**
     * Creates an offline probe sized for the requested path MTU.
     *
     * @param magic offline marker to encode
     * @param protocolVersion transport version offered to the server
     * @param mtu target buffer size, large enough for the marker and version
     */
    public ConnectionRequest1(final RakNet.Magic magic, final int protocolVersion, final int mtu) {
        this.magic = magic;
        this.protocolVersion = protocolVersion;
        this.mtu = mtu;
    }

    /**
     * Appends magic and protocol version, then pads the buffer to the configured probe size. The codec writes the packet ID separately.
     *
     * @param buf destination whose writer index advances, retained by the caller
     */
    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        buf.writeByte(protocolVersion);
        buf.writeZero(mtu - buf.readableBytes());
    }

    /**
     * Infers the MTU from remaining bytes, reads magic and version, and consumes all padding.
     * Sizes below 128 are rejected and sizes above 8192 are clamped to 8192. The packet ID must already have been consumed.
     *
     * @param buf source whose reader index advances, retained by the caller
     * @throws IllegalArgumentException if the decoded probe has fewer than 128 remaining bytes
     */
    @Override
    public void decode(final ByteBuf buf) {
        mtu = buf.readableBytes();
        magic = DefaultMagic.decode(buf);
        protocolVersion = buf.readByte();
        buf.skipBytes(buf.readableBytes());

        if (mtu < 128) {
            throw new IllegalArgumentException("ConnectionRequest1 MTU is too small");
        } else if (mtu > 8192) {
            mtu = 8192; //near hard limit set by 13 bit size in Frame
        }
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
     * Exposes the proposed RakNet version. Decoding sign-extends the one-byte wire value.
     *
     * @return stored transport version, distinct from the application protocol version
     */
    public int getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Selects the transport version written by the probe without validating support.
     *
     * @param protocolVersion proposed version whose low eight bits are encoded
     */
    public void setProtocolVersion(final int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    /**
     * Reports the probe size used for padding or inferred from the received body.
     * The decoder measures after the packet ID has been consumed, so this is not an IP-layer MTU.
     *
     * @return stored probe size in bytes
     */
    public int getMtu() {
        return mtu;
    }

    /**
     * Sets the padding target without validating whether the marker and version fit.
     *
     * @param mtu total readable buffer size to reach during encoding, including any packet ID already present
     */
    public void setMtu(final int mtu) {
        this.mtu = mtu;
    }

}
