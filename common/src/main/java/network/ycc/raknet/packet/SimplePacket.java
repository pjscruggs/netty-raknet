package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Provides body encoding and common RakNet string/address primitives.
 * The registry writes or consumes packet IDs separately. Buffer ownership remains with each caller.
 */
public abstract class SimplePacket implements Packet {

    /** Wildcard endpoint with port zero used to fill unused handshake address slots. */
    private static final InetSocketAddress NULL_ADDR = new InetSocketAddress(0);

    /**
     * Appends a 16-bit byte length followed by UTF-8 data without truncating the text.
     * Callers must keep the byte length representable by the peer's decoder.
     *
     * @param buf destination whose writer index advances
     * @param str text converted to UTF-8 before its byte length is encoded
     */
    public static void writeString(final ByteBuf buf, final String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buf.writeShort(bytes.length);
        buf.writeBytes(bytes);
    }

    /**
     * Reads a signed-short byte count and decodes that many bytes as UTF-8.
     * Lengths with the high bit set are not accepted by this implementation.
     *
     * @param buf source whose reader index advances over the length and encoded text
     * @return decoded UTF-8 text
     */
    public static String readString(final ByteBuf buf) {
        byte[] bytes = new byte[buf.readShort()];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads a RakNet IPv4 or IPv6 socket address.
     * IPv4 address bits are inverted on the wire. IPv6 family, flow and scope fields are consumed but ignored.
     *
     * @param buf source positioned at the address-family byte
     * @return resolved address and unsigned port from the wire
     * @throws DecoderException if the family is neither four nor six, or address construction fails
     */
    public static InetSocketAddress readAddress(final ByteBuf buf) {
        final int type = buf.readByte();
        byte[] addr;
        int port;
        if (type == 4) {
            int addri = ~buf.readInt();
            addr = ByteBuffer.allocate(4).putInt(addri).array();
            port = buf.readUnsignedShort();
        } else if (type == 6) {
            //sockaddr_in6 structure
            buf.skipBytes(2); //family
            port = buf.readUnsignedShort();
            buf.skipBytes(4); //flow info
            addr = new byte[16];
            buf.readBytes(addr);
            buf.skipBytes(4); //scope id
        } else {
            throw new DecoderException("Unknown inet addr version: " + type);
        }
        try {
            return new InetSocketAddress(InetAddress.getByAddress(addr), port);
        } catch (UnknownHostException e) {
            throw new DecoderException("Unexpected error", e);
        }
    }

    /**
     * Appends a wildcard address and port zero for an unused handshake address slot.
     *
     * @param buf destination whose writer index advances
     */
    public static void writeAddress(final ByteBuf buf) {
        writeAddress(buf, NULL_ADDR);
    }

    /**
     * Appends a resolved IPv4 or IPv6 endpoint in RakNet wire format.
     * IPv6 flow information and scope are encoded as zero, so scoped-interface identity is not preserved.
     *
     * @param buf destination whose writer index advances
     * @param address resolved endpoint to serialize
     * @throws EncoderException if the resolved address is neither IPv4 nor IPv6
     */
    public static void writeAddress(final ByteBuf buf, final InetSocketAddress address) {
        final InetAddress addr = address.getAddress();
        if (addr instanceof Inet4Address) {
            buf.writeByte((byte) 4);
            final int addri = ByteBuffer.wrap(addr.getAddress()).getInt();
            buf.writeInt(~addri);
            buf.writeShort(address.getPort());
        } else if (addr instanceof Inet6Address) {
            buf.writeByte((byte) 6);
            //socaddr_in6 structure
            buf.writeShort(10); //family AF_INET6
            buf.writeShort(address.getPort());
            buf.writeInt(0); //flow info
            buf.writeBytes(addr.getAddress());
            buf.writeInt(0); //scope id
        } else {
            throw new EncoderException("Unknown inet addr version: " + addr.getClass().getName());
        }
    }

    /**
     * Appends this packet's body without writing its registry packet ID.
     * Implementations must not release the destination buffer.
     *
     * @param buf caller-owned destination positioned after the packet ID
     */
    public abstract void encode(ByteBuf buf);

    /**
     * Populates this packet from a body whose registry packet ID has already been consumed.
     * Implementations advance the source reader index without releasing it.
     *
     * @param buf caller-owned source containing the packet body
     */
    public abstract void decode(ByteBuf buf);

    /**
     * Identifies the packet type for diagnostics without serializing its fields.
     *
     * @return simple runtime class name followed by empty parentheses
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + "()";
    }

}
