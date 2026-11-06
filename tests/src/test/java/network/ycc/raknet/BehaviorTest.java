package network.ycc.raknet;

import network.ycc.raknet.server.channel.RakNetChildChannel;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

import network.ycc.raknet.client.RakNetClient;
import network.ycc.raknet.config.DefaultMagic;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.InvalidVersion;
import network.ycc.raknet.server.RakNetServer;
import network.ycc.raknet.utils.EmptyInit;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.CorruptedFrameException;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.concurrent.ExecutionException;

/** Exercises connection establishment, rejected negotiation and malformed frame decoding on loopback. */
@Timeout(120)
class BehaviorTest {
    /** Event loops that own this fixture's listening and connecting UDP channels. */
    final EventLoopGroup ioGroup = new NioEventLoopGroup(2);
    /** Separate event loops for server child-connection processing. */
    final EventLoopGroup childGroup = new NioEventLoopGroup(2);
    /** Loopback port shared by these sequential connection scenarios. */
    final int port = 31747;
    /** Default loopback endpoint for the IPv4 and negotiation-failure scenarios. */
    final InetSocketAddress localhost = new InetSocketAddress("localhost", port);
    /** Deliberately incorrect offline marker used to exercise handshake validation. */
    final RakNet.Magic badMagic = new DefaultMagic(new byte[16]);

    /** Stops both fixture-owned event-loop groups even when setup or an assertion fails. */
    @AfterEach
    public void stopEventLoops() {
        ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        childGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        ioGroup.terminationFuture().syncUninterruptibly();
        childGroup.terminationFuture().syncUninterruptibly();
    }

    /**
     * Establishes both peer channels over IPv6 when the host supports that address family.
     * @throws Throwable if a supported IPv6 handshake or channel cleanup fails
     */
    @Test
    void connectIPv6() throws Throwable {
        try {
            final InetSocketAddress localhostIPv6 = new InetSocketAddress("::1", port);

            tryConnect(localhostIPv6);
        } catch (UnsupportedAddressTypeException e) {
            System.out.println("No IPv6 support, skipping.");
        }
    }

    /**
     * Establishes both peer channels over the normal loopback endpoint.
     * @throws Throwable if a supported address-family handshake or channel cleanup fails
     */
    @Test
    void connectIPv4() throws Throwable {
        try {
            tryConnect(localhost);
        } catch (UnsupportedAddressTypeException e) {
            System.out.println("No IPv4 support, skipping.");
        }
    }

    /**
     * Waits for client and server transport negotiation before checking their active states.
     * @param localhost loopback endpoint selecting the address family under test
     * @throws InterruptedException if a connection or close wait is interrupted
     * @throws ExecutionException if retrieving the server's child connection fails
     */
    private void tryConnect(final InetSocketAddress localhost) throws InterruptedException, ExecutionException {
        final Channel serverChannel = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channel(RakNetServer.CHANNEL)
                .childHandler(new EmptyInit())
                .bind(localhost).sync().channel();

        final Channel clientChannel = new Bootstrap()
                .group(ioGroup)
                .channel(RakNetClient.THREADED_CHANNEL)
                .handler(new EmptyInit())
                .connect(localhost).sync().channel();

        final RakNetChildChannel childChannel = serverChannel.eventLoop().submit(() -> (RakNetChildChannel) ((RakNetServerChannel) serverChannel).getChildChannel(clientChannel.localAddress())).get();
        childChannel.connectFuture().sync();

        try {
            Assertions.assertTrue(clientChannel.isActive());
            Assertions.assertTrue(childChannel.isActive());
//            Assertions.assertTrue(childChannel.getApplicationChannel().isActive()); // TODO wait for application channel to be active
        } finally {
            serverChannel.close().sync();
            clientChannel.close().sync();
        }
    }

    /**
     * Rejects a client that sends an incorrect offline marker and closes both fixture channels.
     * @throws Throwable if setup or cleanup fails outside the expected negotiation rejection
     */
    @Test()
    void badMagicClient() throws Throwable {
        final Channel serverChannel = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channel(RakNetServer.CHANNEL)
                .childHandler(new EmptyInit())
                .bind(localhost).sync().channel();

        final ChannelFuture clientConnect = new Bootstrap()
                .group(ioGroup)
                .channel(RakNetClient.THREADED_CHANNEL)
                .option(RakNet.MAGIC, badMagic)
                .handler(new EmptyInit())
                .connect(localhost);

        final Channel clientChannel = clientConnect.channel();

        Assertions.assertThrows(ChannelException.class, () -> {
            try {
                clientConnect.sync();
            } finally {
                serverChannel.close().sync();
                clientChannel.close().sync();
            }
        });
    }


    /**
     * Verifies the connect deadline when an ordinary UDP listener never answers the RakNet handshake.
     * @throws Throwable if setup or cleanup fails outside the expected timeout
     */
    @Test()
    void badConnect() throws Throwable {
        final Channel serverChannel = new Bootstrap()
                .group(ioGroup)
                .channel(NioDatagramChannel.class)
                .handler(new EmptyInit())
                .bind(localhost).sync().channel();

        final ChannelFuture clientConnect = new Bootstrap()
                .group(ioGroup)
                .channel(RakNetClient.THREADED_CHANNEL)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
                .handler(new EmptyInit())
                .connect(localhost);

        final Channel clientChannel = clientConnect.channel();

        Assertions.assertThrows(ConnectTimeoutException.class, () -> {
            try {
                clientConnect.sync();
            } finally {
                serverChannel.close().sync();
                clientChannel.close().sync();
            }
        });
    }

    /**
     * Exercises rejection of a client offering a transport version outside the server's accepted list.
     * @throws Throwable if setup or cleanup fails outside the expected version rejection
     */
    @Test
    void badVersionClient() throws Throwable {
        final Channel serverChannel = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channel(RakNetServer.CHANNEL)
                .childHandler(new EmptyInit())
                .bind(localhost).sync().channel();

        final ChannelFuture clientConnect = new Bootstrap()
                .group(ioGroup)
                .channel(RakNetClient.THREADED_CHANNEL)
                .option(RakNet.PROTOCOL_VERSION, 1)
                .handler(new EmptyInit())
                .connect(localhost);

        final Channel clientChannel = clientConnect.channel();

        Assertions.assertThrows(InvalidVersion.InvalidVersionException.class, () -> {
            try {
                clientConnect.sync();
            } finally {
                serverChannel.close().sync();
                clientChannel.close().sync();
            }
        });
    }

    /**
     * Rejects the normal client version when the server accepts only a different transport version.
     * @throws Throwable if setup or cleanup fails outside the expected version rejection
     */
    @Test
    void badVersionServer() throws Throwable {
        final Channel serverChannel = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channel(RakNetServer.CHANNEL)
                .option(RakNet.PROTOCOL_VERSION, 1)
                .childHandler(new EmptyInit())
                .bind(localhost).sync().channel();

        RakNet.config(serverChannel).setprotocolVersions(new int[]{1});

        final ChannelFuture clientConnect = new Bootstrap()
                .group(ioGroup)
                .channel(RakNetClient.THREADED_CHANNEL)
                .handler(new EmptyInit())
                .connect(localhost);

        final Channel clientChannel = clientConnect.channel();

        Assertions.assertThrows(InvalidVersion.InvalidVersionException.class, () -> {
            try {
                clientConnect.sync();
            } finally {
                serverChannel.close().sync();
                clientChannel.close().sync();
            }
        });
    }

    /** A truncated frame-set payload must produce a decoder failure rather than a partially accepted packet. */
    @Test()
    void corruptFrameTest() {
        final ByteBuf input = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        try {
            Assertions.assertThrows(CorruptedFrameException.class, () -> FrameSet.read(input));
        } finally {
            input.release();
        }
    }
}
