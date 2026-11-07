package network.ycc.raknet;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import network.ycc.raknet.client.channel.RakNetClientThreadedChannel;
import network.ycc.raknet.utils.EmptyInit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the real handshake when a hostname's first endpoint silently discards all datagrams. */
@Timeout(20)
class ClientAddressFallbackTest {
    /**
     * A bound black-hole socket precedes a working server, reproducing a broken preferred network path.
     * @throws Exception if the isolated sockets or bounded handshake cannot be created
     */
    @Test
    void unreachableFirstAddressDoesNotHideWorkingServer() throws Exception {
        verifyFallback("127.0.0.1");
    }

    /**
     * The same client socket must negotiate with IPv6 when the first IPv4 endpoint never replies.
     * @throws Exception if the isolated dual-stack sockets or bounded handshake cannot be created
     */
    @Test
    void unreachableIpv4CanUseWorkingIpv6() throws Exception {
        verifyFallback("::1");
    }

    /**
     * Runs one real handshake with a silent first endpoint and a server in the requested address family.
     * @param target numeric loopback address for the responding endpoint
     * @throws Exception if socket setup, handshake verification or cleanup fails
     */
    private static void verifyFallback(final String target) throws Exception {
        final EndToEndTest fixture = new EndToEndTest();
        Channel server = null;
        Channel client = null;
        try (DatagramSocket blackhole = new DatagramSocket(0, InetAddress.getByAddress(new byte[] {127, 0, 0, 1}))) {
            server = new ServerBootstrap().group(fixture.ioGroup, fixture.childGroup)
                    .channel(RakNetServerChannel.class).childHandler(new EmptyInit())
                    .bind(new InetSocketAddress(InetAddress.getAllByName(target)[0], 0)).sync().channel();
            final InetSocketAddress second = (InetSocketAddress) server.localAddress();
            final InetSocketAddress first = (InetSocketAddress) blackhole.getLocalSocketAddress();
            final ChannelFuture connection = new Bootstrap().group(fixture.ioGroup)
                    .channelFactory(() -> client(first, second))
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                    .handler(new EmptyInit()).connect(first);
            client = connection.channel();
            assertTrue(connection.await(5, TimeUnit.SECONDS), "Handshake did not finish within its deadline");
            assertTrue(connection.isSuccess(), "A working alternate must complete the RakNet handshake: " + connection.cause());
            assertEquals(second, client.remoteAddress(), "Only the winning endpoint may become the application peer");
        } finally {
            if (client != null) {
                client.close().sync();
            }
            if (server != null) {
                server.close().sync();
            }
            fixture.stopEventLoops();
        }
    }

    /**
     * Configures both candidates before registration without installing any Minecraft login handlers.
     * @param first bound socket that intentionally never responds
     * @param second real server that must win the offline handshake
     * @return unregistered application channel with one owned UDP transport
     */
    private static RakNetClientThreadedChannel client(final InetSocketAddress first, final InetSocketAddress second) {
        final RakNetClientThreadedChannel client = new RakNetClientThreadedChannel();
        client.parent().setRemoteAddresses(Arrays.asList(first, second));
        return client;
    }
}
