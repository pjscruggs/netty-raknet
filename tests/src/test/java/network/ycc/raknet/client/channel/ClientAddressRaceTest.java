package network.ycc.raknet.client.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;
import network.ycc.raknet.config.DefaultConfig;
import network.ycc.raknet.packet.ConnectionReply1;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Uses an explicit clock and real reply encoding to verify address selection independently of DNS. */
class ClientAddressRaceTest {
    /** First endpoint used by scheduling and peer-pinning tests. */
    private static final InetSocketAddress FIRST = new InetSocketAddress("127.0.0.1", 25565);
    /** Alternate endpoint whose reply must not be trusted before a probe is sent to it. */
    private static final InetSocketAddress SECOND = new InetSocketAddress("127.0.0.2", 25565);

    /** Candidates are staggered by 250 milliseconds without depending on wall-clock test scheduling. */
    @Test
    void probesRespectAttemptDelay() {
        final ClientAddressRace race = new ClientAddressRace(Arrays.asList(FIRST, SECOND));
        assertEquals(FIRST, race.destination(0));
        assertEquals(FIRST, race.destination(TimeUnit.MILLISECONDS.toNanos(249)));
        assertEquals(SECOND, race.destination(TimeUnit.MILLISECONDS.toNanos(250)));
        assertEquals(FIRST, race.destination(TimeUnit.MILLISECONDS.toNanos(500)));
        assertNull(race.selected());
    }

    /** A failed address family leaves alternatives eligible, and a successful reply supersedes late failures. */
    @Test
    void oneLocalFailureDoesNotFailAllCandidates() {
        final ClientAddressRace race = new ClientAddressRace(Arrays.asList(FIRST, SECOND));
        assertFalse(race.failed(FIRST));
        assertFalse(race.failed(FIRST));
        assertTrue(race.failed(SECOND));
    }

    /** Only a valid reply from a probed endpoint can pin the peer, after which other addresses are rejected. */
    @Test
    void validReplyPinsOnlyAnAttemptedPeer() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final DefaultConfig config = new DefaultConfig(channel);
        final ClientAddressRace race = new ClientAddressRace(Arrays.asList(FIRST, SECOND));
        try {
            race.destination(0);
            assertFalse(reply(race, config, SECOND, false));
            assertFalse(reply(race, config, FIRST, true));
            assertNull(race.selected());
            race.destination(TimeUnit.MILLISECONDS.toNanos(250));
            assertTrue(reply(race, config, SECOND, false));
            assertEquals(SECOND, race.selected());
            assertEquals(SECOND, race.destination(TimeUnit.SECONDS.toNanos(10)));
            assertFalse(reply(race, config, FIRST, false));
            assertFalse(race.failed(FIRST));
            assertFalse(race.failed(SECOND));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Encodes a real first reply and verifies that selection neither consumes nor retains its envelope.
     * @param race connection-local candidate selection under test
     * @param config actual transport codec and magic used to construct the reply
     * @param sender endpoint claiming to answer the probe
     * @param corruptMagic whether to replace a magic byte while retaining a syntactically valid reply
     * @return selection result, after releasing the test-owned datagram
     */
    private static boolean reply(final ClientAddressRace race, final DefaultConfig config,
            final InetSocketAddress sender, final boolean corruptMagic) {
        final ByteBuf bytes = io.netty.buffer.Unpooled.buffer();
        config.getCodec().encode(new ConnectionReply1(config.getMagic(), 1400, 17), bytes);
        final DatagramPacket packet = new DatagramPacket(bytes, FIRST, sender);
        try {
            if (corruptMagic) {
                bytes.setByte(1, bytes.getByte(1) ^ 1);
            }
            final boolean accepted = race.accept(packet, config);
            assertEquals(0, bytes.readerIndex());
            assertEquals(1, bytes.refCnt());
            return accepted;
        } finally {
            packet.release();
        }
    }
}
