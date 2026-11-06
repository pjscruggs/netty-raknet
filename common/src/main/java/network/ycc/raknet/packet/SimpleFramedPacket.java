package network.ycc.raknet.packet;

public abstract class SimpleFramedPacket extends SimplePacket implements FramedPacket {

    protected FramedPacket.Reliability reliability = FramedPacket.Reliability.RELIABLE_ORDERED;
    protected int orderChannel = 0;

    @Override
    public Reliability getReliability() {
        return reliability;
    }

    @Override
    public void setReliability(final FramedPacket.Reliability reliability) {
        this.reliability = reliability;
    }

    @Override
    public int getOrderChannel() {
        return orderChannel;
    }

    @Override
    public void setOrderChannel(final int orderChannel) {
        this.orderChannel = orderChannel;
    }

}
