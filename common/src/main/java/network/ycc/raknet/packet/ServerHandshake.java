package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;

public class ServerHandshake extends SimpleFramedPacket {

    private InetSocketAddress clientAddr;
    private long timestamp;
    private int nExtraAddresses;

    public ServerHandshake() {
        reliability = Reliability.RELIABLE;
    }

    public ServerHandshake(final InetSocketAddress clientAddr, final long timestamp) {
        this(clientAddr, timestamp, 20);
    }

    public ServerHandshake(final InetSocketAddress clientAddr, final long timestamp, final int nExtraAddresses) {
        this();
        this.clientAddr = clientAddr;
        this.timestamp = timestamp;
        this.nExtraAddresses = nExtraAddresses;
    }

    @Override
    public void encode(final ByteBuf buf) {
        writeAddress(buf, clientAddr);
        buf.writeShort(0);
        for (int i = 0; i < nExtraAddresses; i++) {
            writeAddress(buf);
        }
        buf.writeLong(timestamp);
        buf.writeLong(System.currentTimeMillis());
    }

    @Override
    public void decode(final ByteBuf buf) {
        clientAddr = readAddress(buf);
        buf.readShort();
        for (nExtraAddresses = 0; buf.readableBytes() > 16; nExtraAddresses++) {
            readAddress(buf);
        }
        timestamp = buf.readLong();
        timestamp = buf.readLong();
    }

    public InetSocketAddress getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(final InetSocketAddress clientAddr) {
        this.clientAddr = clientAddr;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public int getnExtraAddresses() {
        return nExtraAddresses;
    }

    public void setnExtraAddresses(final int nExtraAddresses) {
        this.nExtraAddresses = nExtraAddresses;
    }

}
