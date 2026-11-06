package network.ycc.raknet.channel;

import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises channel collections with both RakNet pipelines and their backing UDP sockets. */
class DatagramChannelIdentityTest {
    /** Sorted and hashed collections must retain each pipeline, including its distinct parent. */
    @Test
    void proxiesAndParentsHaveConsistentDistinctIdentities() {
        final NioEventLoopGroup group = new NioEventLoopGroup(1);
        final DatagramChannelProxy first = new DatagramChannelProxy(NioDatagramChannel.class);
        final DatagramChannelProxy second = new DatagramChannelProxy(NioDatagramChannel.class);
        try {
            group.register(first.parent()).syncUninterruptibly();
            group.register(second.parent()).syncUninterruptibly();
            final Channel[] channels = {first, first.parent(), second, second.parent()};
            assertEquals(4, new TreeSet<>(Arrays.asList(channels)).size());
            assertEquals(4, new HashSet<>(Arrays.asList(channels)).size());
            for (final Channel left : channels) {
                for (final Channel right : channels) {
                    assertIdentity(left, right);
                }
            }
        } finally {
            first.parent().close().syncUninterruptibly();
            second.parent().close().syncUninterruptibly();
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /**
     * Checks equality symmetry and ordering antisymmetry for each pair of actual Netty channels.
     *
     * @param left first pipeline or socket
     * @param right second pipeline or socket
     */
    private static void assertIdentity(final Channel left, final Channel right) {
        assertEquals(left == right, left.equals(right));
        assertEquals(left == right, left.compareTo(right) == 0);
        assertEquals(Integer.signum(left.compareTo(right)), -Integer.signum(right.compareTo(left)));
        assertTrue(left.equals(left));
        assertFalse(left.equals(null));
    }
}
