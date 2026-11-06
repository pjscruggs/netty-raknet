package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

public class Pong extends SimpleFramedPacket {

    private long pingTimestamp;
    private long pongTimestamp;

    public Pong() {
        reliability = Reliability.UNRELIABLE;
    }

    public Pong(final long pingTimestamp, final Reliability reliability) {
        this(pingTimestamp);
        this.reliability = reliability;
    }

    public Pong(final long pingTimestamp) {
        this();
        this.pingTimestamp = pingTimestamp;
        this.pongTimestamp = System.nanoTime();
    }

    @Override
    public void encode(final ByteBuf buf) {
        buf.writeLong(pingTimestamp);
        buf.writeLong(pongTimestamp);
    }

    @Override
    public void decode(final ByteBuf buf) {
        pingTimestamp = buf.readLong();
        if (buf.isReadable()) {
            pongTimestamp = buf.readLong();
        }
    }

    public long getPingTimestamp() {
        return pingTimestamp;
    }

    public long getPongTimestamp() {
        return pongTimestamp;
    }

    public long getRTT() {
        return System.nanoTime() - pingTimestamp;
    }

}