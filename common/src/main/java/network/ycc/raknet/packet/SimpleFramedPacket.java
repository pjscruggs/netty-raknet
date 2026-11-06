package network.ycc.raknet.packet;

/** Adds mutable delivery metadata to simple packet bodies before frame ordering and fragmentation. */
public abstract class SimpleFramedPacket extends SimplePacket implements FramedPacket {

    /** Requested transport policy, initially reliable ordered delivery. */
    protected FramedPacket.Reliability reliability = FramedPacket.Reliability.RELIABLE_ORDERED;
    /** Ordering lane used by ordered or sequenced delivery, initially zero. */
    protected int orderChannel;

    /**
     * Returns the delivery policy copied into the encoded frame metadata.
     *
     * @return current reliability and ordering policy
     */
    @Override
    public Reliability getReliability() {
        return reliability;
    }

    /**
     * Selects the delivery policy for later framing without changing the packet body.
     *
     * @param reliability non-null transport policy to copy into frame metadata
     */
    @Override
    public void setReliability(final FramedPacket.Reliability reliability) {
        this.reliability = reliability;
    }

    /**
     * Returns the lane used when the selected policy carries ordering metadata.
     *
     * @return current ordering channel, initially zero
     */
    @Override
    public int getOrderChannel() {
        return orderChannel;
    }

    /**
     * Selects an ordering lane without validating it at this packet layer.
     * Callers must use a lane supported by the transport, which currently provides eight lanes.
     *
     * @param orderChannel ordering lane for later framing
     */
    @Override
    public void setOrderChannel(final int orderChannel) {
        this.orderChannel = orderChannel;
    }

}
