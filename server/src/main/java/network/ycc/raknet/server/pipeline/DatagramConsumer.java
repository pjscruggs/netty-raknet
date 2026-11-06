package network.ycc.raknet.server.pipeline;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

/**
 * Releases unmatched datagrams at the listener tail while preserving accepted-channel notifications.
 */
@Sharable
public class DatagramConsumer extends ChannelInboundHandlerAdapter {

    /**
     * Pipeline name used to place the discard handler after the server's connection acceptor.
     */
    public static final String NAME = "rn-datagram-consumer";
    /**
     * Stateless shared instance that owns no connection-specific resources.
     */
    public static final DatagramConsumer INSTANCE = new DatagramConsumer();

    /**
     * Consumes the inbound reference of an unmatched datagram and forwards other message types unchanged.
     *
     * @param ctx listener context used to forward accepted channels or other non-datagram messages
     * @param msg inbound message whose ownership transfers to this handler
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof DatagramPacket) {
            ReferenceCountUtil.safeRelease(msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

}
