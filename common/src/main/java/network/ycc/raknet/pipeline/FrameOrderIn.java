package network.ycc.raknet.pipeline;

import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.utils.Constants;
import network.ycc.raknet.utils.UINT;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;

import java.util.Arrays;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class FrameOrderIn extends MessageToMessageDecoder<Frame> {

    public static final String NAME = "rn-order-in";

    protected final OrderedChannelPacketQueue[] channels = new OrderedChannelPacketQueue[8];

    public FrameOrderIn() {
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new OrderedChannelPacketQueue();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        Arrays.stream(channels).forEach(OrderedChannelPacketQueue::clear);
    }

    /**
     * Routes a frame only after validating the peer-controlled ordering channel.
     *
     * @param ctx connection closed when a frame selects an unsupported channel
     * @param frame input released by the decoder after successful or failed processing
     * @param list accepted payload references transferred to the next handler
     * @throws CorruptedFrameException if ordered traffic selects a channel outside the protocol
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, final Frame frame, final List<Object> list) {
        final int channel = frame.getOrderChannel();
        if (frame.getReliability().isOrdered && (channel < 0 || channel >= channels.length)) {
            ctx.close();
            throw new CorruptedFrameException("Invalid RakNet ordering channel: " + channel);
        }
        if (frame.getReliability().isSequenced) {
            frame.touch("Sequenced");
            channels[channel].decodeSequenced(frame, list);
        } else if (frame.getReliability().isOrdered) {
            frame.touch("Ordered");
            channels[channel].decodeOrdered(frame, list);
        } else {
            frame.touch("No order");
            list.add(frame.retainedFrameData());
        }
    }

    protected static class OrderedChannelPacketQueue {

        protected final Int2ObjectOpenHashMap<FramedPacket> queue = new Int2ObjectOpenHashMap<>();
        protected int lastOrderIndex = -1;
        protected int lastSequenceIndex = -1;

        protected void decodeSequenced(Frame frame, List<Object> list) {
            if (UINT.B3.minusWrap(frame.getSequenceIndex(), lastSequenceIndex) > 0) {
                lastSequenceIndex = frame.getSequenceIndex();
                //remove earlier packets from queue
                while (UINT.B3.minusWrap(frame.getOrderIndex(), lastOrderIndex) > 1) {
                    ReferenceCountUtil.release(queue.remove(lastOrderIndex));
                    lastOrderIndex = UINT.B3.plus(lastOrderIndex, 1);
                }
            }
            decodeOrdered(frame, list); //register packet as normal
        }

        protected void decodeOrdered(Frame frame, List<Object> list) {
            final int indexDiff = UINT.B3.minusWrap(frame.getOrderIndex(), lastOrderIndex);
            Constants.packetLossCheck(indexDiff, "ordered difference");
            if (indexDiff == 1) { //got next packet in line
                FramedPacket data = frame.retainedFrameData();
                do { //process this packet, and any queued packets following in sequence
                    list.add(data);
                    lastOrderIndex = UINT.B3.plus(lastOrderIndex, 1);
                    data = queue.remove(UINT.B3.plus(lastOrderIndex, 1));
                } while (data != null);
            } else if (indexDiff > 1 && !queue.containsKey(frame.getOrderIndex())) {
                // only new future data goes in the queue
                queue.put(frame.getOrderIndex(), frame.retainedFrameData());
            }
            Constants.packetLossCheck(queue.size(), "missed ordered packets");
        }

        protected void clear() {
            queue.values().forEach(ReferenceCountUtil::release);
            queue.clear();
        }

    }

}
