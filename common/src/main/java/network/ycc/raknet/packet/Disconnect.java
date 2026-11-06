package network.ycc.raknet.packet;

import io.netty.buffer.ByteBuf;

/** Requests disconnection using an empty reliably ordered control packet on channel seven. */
public class Disconnect extends SimpleFramedPacket {

    /** Selects reliable ordered delivery on the dedicated disconnect ordering channel. */
    public Disconnect() {
        reliability = Reliability.RELIABLE_ORDERED;
        orderChannel = 7;
    }

    /**
     * Writes no body because the codec's packet ID completely represents this control message.
     *
     * @param buf caller-owned destination, left unchanged
     */
    @Override
    public void encode(final ByteBuf buf) {
        // NOOP
    }

    /**
     * Consumes no body for the disconnect control message.
     *
     * @param buf caller-owned source, left unchanged
     */
    @Override
    public void decode(final ByteBuf buf) {
        // NOOP
    }

}
