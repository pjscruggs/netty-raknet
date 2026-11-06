package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

public class Disconnect extends SimpleFramedPacket {

    public Disconnect() {
        reliability = Reliability.RELIABLE_ORDERED;
        orderChannel = 7;
    }

    @Override
    public void encode(final ByteBuf buf) {
        // NOOP
    }

    @Override
    public void decode(final ByteBuf buf) {
        // NOOP
    }

}
