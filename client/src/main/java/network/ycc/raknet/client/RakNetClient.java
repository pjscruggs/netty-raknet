package network.ycc.raknet.client;

import io.netty.channel.Channel;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.client.channel.RakNetClientChannel;
import network.ycc.raknet.client.channel.RakNetClientThreadedChannel;
import network.ycc.raknet.client.pipeline.ConnectionInitializer;
import network.ycc.raknet.pipeline.FlushTickHandler;
import network.ycc.raknet.pipeline.RawPacketCodec;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;

/**
 * Supplies client channel types and the transport pipeline installed before connection negotiation.
 */
public class RakNetClient extends RakNet {

    /**
     * Bootstrap channel type that runs the RakNet pipeline on the UDP channel's event loop.
     */
    public static final Class<RakNetClientChannel> CHANNEL = RakNetClientChannel.class;
    /**
     * Bootstrap channel type that separates application handlers from the parent transport pipeline.
     */
    public static final Class<RakNetClientThreadedChannel> THREADED_CHANNEL = RakNetClientThreadedChannel.class;

    /**
     * Installs framing and packet handlers while reserving a named slot for the connection handshake.
     */
    public static class DefaultClientInitializer extends ChannelInitializer<Channel> {
        /**
         * Reusable initializer that creates connection-specific handlers for each new client pipeline.
         */
        public static final DefaultClientInitializer INSTANCE = new DefaultClientInitializer();

        /**
         * Adds flush scheduling, wire decoding and reliable framing before the handshake placeholder.
         * The client connect handler replaces the placeholder after its UDP socket connects.
         *
         * @param channel newly registered client channel whose pipeline is initialized once
         */
        @Override
        protected void initChannel(final Channel channel) {
            channel.pipeline()
                    .addLast(FlushTickHandler.NAME, new FlushTickHandler())
                    .addLast(RawPacketCodec.NAME, RawPacketCodec.INSTANCE)
                    .addLast(ReliableFrameHandling.INSTANCE)
                    .addLast(PacketHandling.INSTANCE)
                    .addLast(ConnectionInitializer.NAME, new ChannelInboundHandlerAdapter()); //will be removed
        }
    }

}
