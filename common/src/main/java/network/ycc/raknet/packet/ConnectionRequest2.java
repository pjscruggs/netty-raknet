package network.ycc.raknet.packet;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.config.DefaultMagic;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

public class ConnectionRequest2 extends SimplePacket implements Packet, Packet.ClientIdConnection {

    private RakNet.Magic magic;
    private int mtu;
    private long clientId;
    private InetSocketAddress address;

    public ConnectionRequest2() {
    }

    public ConnectionRequest2(final RakNet.Magic magic, final int mtu, final long clientId,
            final InetSocketAddress address) {
        this.magic = magic;
        this.mtu = mtu;
        this.clientId = clientId;
        this.address = address;
    }

    @Override
    public void encode(final ByteBuf buf) {
        magic.write(buf);
        writeAddress(buf, address);
        buf.writeShort(mtu);
        buf.writeLong(clientId);
    }

    @Override
    public void decode(final ByteBuf buf) {
        magic = DefaultMagic.decode(buf);
        address = readAddress(buf);
        mtu = buf.readUnsignedShort();
        clientId = buf.readLong();
    }

    public RakNet.Magic getMagic() {
        return magic;
    }

    public void setMagic(final RakNet.Magic magic) {
        this.magic = magic;
    }

    public int getMtu() {
        return mtu;
    }

    public void setMtu(final int mtu) {
        this.mtu = mtu;
    }

    @Override
    public long getClientId() {
        return clientId;
    }

    public void setClientId(final long clientId) {
        this.clientId = clientId;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(final InetSocketAddress address) {
        this.address = address;
    }

}
