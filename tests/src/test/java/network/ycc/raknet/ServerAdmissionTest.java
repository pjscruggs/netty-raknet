package network.ycc.raknet;

import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises admission at the exact configured limit through a real listener's connect pipeline. */
@Timeout(20)
class ServerAdmissionTest {
    /**
     * Zero capacity refuses the first peer, while a negative limit is rejected without changing capacity.
     *
     * @throws Exception if listener setup or event-loop verification fails
     */
    @Test
    void zeroCapacityRefusesFirstPeer() throws Exception {
        final EndToEndTest fixture = new EndToEndTest();
        try {
            final RakNetServerChannel server = (RakNetServerChannel) fixture.newServer(null, null);
            final InetSocketAddress peer = new InetSocketAddress("127.0.0.1", 31746);
            server.config().setOption(RakNet.MAX_CONNECTIONS, 0);
            assertThrows(IllegalArgumentException.class,
                    () -> server.config().setOption(RakNet.MAX_CONNECTIONS, -1));
            assertEquals(0, RakNet.config(server).getMaxConnections());
            assertRefused(server.connect(peer, server.localAddress()));
            assertNull(server.eventLoop().submit(() -> server.getChildChannel(peer)).get());
            server.close().sync();
        } finally {
            fixture.stopEventLoops();
        }
    }

    /**
     * Repeated admission of an existing endpoint succeeds at capacity without admitting a second endpoint.
     *
     * @throws Exception if listener setup or event-loop verification fails
     */
    @Test
    void fullListenerRetainsExistingPeerButRefusesAnother() throws Exception {
        final EndToEndTest fixture = new EndToEndTest();
        try {
            final RakNetServerChannel server = (RakNetServerChannel) fixture.newServer(null, null);
            server.config().setOption(RakNet.MAX_CONNECTIONS, 1);
            final InetSocketAddress first = new InetSocketAddress("127.0.0.1", 31746);
            final InetSocketAddress second = new InetSocketAddress("127.0.0.1", 31747);
            server.connect(first, server.localAddress()).sync();
            final Object accepted = server.eventLoop().submit(() -> server.getChildChannel(first)).get();
            server.connect(first, server.localAddress()).sync();
            assertSame(accepted, server.eventLoop().submit(() -> server.getChildChannel(first)).get());
            assertRefused(server.connect(second, server.localAddress()));
            assertNull(server.eventLoop().submit(() -> server.getChildChannel(second)).get());
            server.close().sync();
        } finally {
            fixture.stopEventLoops();
        }
    }

    /**
     * Requires the admission operation to fail promptly with the connection-limit error.
     *
     * @param admission child-creation promise returned by the listener connect operation
     * @throws InterruptedException if the bounded completion wait is interrupted
     */
    private static void assertRefused(final ChannelFuture admission) throws InterruptedException {
        assertTrue(admission.await(5, TimeUnit.SECONDS));
        assertFalse(admission.isSuccess());
        assertEquals("Too many connections", admission.cause().getMessage());
    }
}
