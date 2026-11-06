package network.ycc.raknet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

import network.ycc.raknet.client.RakNetClient;
import network.ycc.raknet.pipeline.UserDataCodec;
import network.ycc.raknet.server.RakNetServer;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;

/** Exercises a minimal server and threaded client exchanging UTF-8 application payloads over RakNet. */
@Timeout(120)
public class TestHelloWorld {

    /** Owns the UDP socket event loops used by this fixture's client and server. */
    final EventLoopGroup ioGroup = new NioEventLoopGroup(2);
    /** Runs server child-connection processing independently of the listening socket. */
    final EventLoopGroup childGroup = new NioEventLoopGroup(2);
    /** Loopback endpoint used only by this integration fixture. */
    final InetSocketAddress localhost = new InetSocketAddress("localhost", 31747);
    /** Expected decoded application text repeated to exercise reliable delivery. */
    final String helloWorld = "Hello world!";

    /** Most recently decoded server payload, read after client write completion and channel shutdown. */
    String resultStr;

    /** Stops both fixture-owned event-loop groups even when setup or an assertion fails. */
    @AfterEach
    public void stopEventLoops() {
        ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        childGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        ioGroup.terminationFuture().syncUninterruptibly();
        childGroup.terminationFuture().syncUninterruptibly();
    }

    /**
     * Completes a real handshake and delivers repeated application buffers through the threaded client.
     *
     * @throws Throwable if binding, connecting, sending or closing a test channel fails
     */
    @Test
    public void helloWorld() throws Throwable {
        final Channel serverChannel = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channel(RakNetServer.CHANNEL)
                .option(RakNet.SERVER_ID,
                        1234567L) //will be set randomly if not specified (optional)
                .childHandler(new ChannelInitializer<Channel>() {
                    /**
                     * Installs the test application identifier and text receiver on an accepted child.
                     * @param ch accepted server-side RakNet channel
                     */
                    @Override
                    protected void initChannel(final Channel ch) {
                        ch.pipeline().addLast(UserDataCodec.NAME, new UserDataCodec(0xFE));
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            /**
                             * Copies readable bytes into text before the enclosing handler releases the buffer.
                             * @param ctx server application's inbound context
                             * @param msg automatically released application payload
                             */
                            @Override
                            protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) {
                                resultStr = msg.readCharSequence(msg.readableBytes(),
                                        StandardCharsets.UTF_8).toString();
                                System.out.println(resultStr); //"Hello world!"
                            }
                        });
                    }
                }).bind(localhost).sync().channel();

        final Channel clientChannel = new Bootstrap()
                .group(ioGroup)
                .channel(RakNetClient.THREADED_CHANNEL)
                .option(RakNet.MTU, 150) //can configure an initial MTU if desired (optional)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        5000) //supports most normal netty ChannelOptions (optional)
                .option(ChannelOption.SO_REUSEADDR, true) //can also set socket options (optional)
                .handler(new ChannelInitializer<Channel>() {
                    /**
                     * Uses the same application packet identifier as the server's payload decoder.
                     * @param ch newly initialized threaded client channel
                     */
                    @Override
                    protected void initChannel(final Channel ch) {
                        ch.pipeline().addLast(UserDataCodec.NAME, new UserDataCodec(0xFE));
                    }
                }).connect(localhost).sync().channel();

        for (int i = 0; i < 8; i ++) {
            final ByteBuf helloWorldBuf = Unpooled.buffer();
            helloWorldBuf.writeCharSequence(helloWorld, StandardCharsets.UTF_8);
            clientChannel.writeAndFlush(helloWorldBuf).sync();
        }

        serverChannel.close().sync();
        clientChannel.close().sync();

        Assertions.assertEquals(resultStr, helloWorld);
    }

}
