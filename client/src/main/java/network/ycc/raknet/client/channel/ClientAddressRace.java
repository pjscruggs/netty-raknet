package network.ycc.raknet.client.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.packet.ConnectionReply1;
import network.ycc.raknet.packet.Packet;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Races only offline handshake probes; a validated first reply pins all subsequent session traffic. */
final class ClientAddressRace {
    /** Minimum spacing between probes that introduce another destination. */
    private static final long ATTEMPT_DELAY = TimeUnit.MILLISECONDS.toNanos(250);
    /** Caller-authorized endpoints, with the original resolver result first. */
    private final List<InetSocketAddress> candidates;
    /** Endpoints actually probed; unsolicited replies cannot select an untried address. */
    private final Set<InetSocketAddress> attempted = new HashSet<>();
    /** Candidates whose offline writes failed locally, counted once toward total path failure. */
    private final Set<InetSocketAddress> failed = new HashSet<>();
    /** Destination selected by a complete, valid first handshake reply, or null while probing. */
    private InetSocketAddress selected;
    /** Index receiving retries until the next destination becomes eligible. */
    private int index;
    /** Monotonic time after which the next destination may receive a probe. */
    private long nextAttempt;

    /**
     * Copies a bounded list of resolved endpoints before the transport event loop starts using it.
     * @param addresses candidates already checked against the application's destination policy
     * @throws IllegalArgumentException if a candidate is unresolved or the list is empty or too large
     */
    ClientAddressRace(final List<InetSocketAddress> addresses) {
        if (addresses.isEmpty() || addresses.size() > 8) {
            throw new IllegalArgumentException("Expected one through eight RakNet destinations");
        }
        candidates = new ArrayList<>();
        for (InetSocketAddress address : addresses) {
            if (address.isUnresolved()) {
                throw new IllegalArgumentException("RakNet destinations must be resolved");
            }
            if (!candidates.contains(address)) {
                candidates.add(address);
            }
        }
    }

    /**
     * Chooses a probe destination without increasing the existing handshake retry frequency.
     * @param now monotonic nanoseconds, supplied explicitly for deterministic scheduling tests
     * @return pinned peer, or the candidate due for an offline probe
     */
    InetSocketAddress destination(final long now) {
        if (selected != null) {
            return selected;
        }
        if (attempted.isEmpty()) {
            nextAttempt = now + ATTEMPT_DELAY;
        } else if (now - nextAttempt >= 0) {
            index = (index + 1) % candidates.size();
            nextAttempt = now + ATTEMPT_DELAY;
        }
        final InetSocketAddress address = candidates.get(index);
        attempted.add(address);
        return address;
    }

    /**
     * Returns the established peer only after a valid reply; callers must not cache a tentative address.
     * @return selected endpoint, or null until the first reply passes validation
     */
    InetSocketAddress selected() {
        return selected;
    }

    /**
     * Records a local probe-write failure without allowing one address family to abort the others.
     * A late failed probe cannot invalidate an endpoint that has already answered successfully.
     * @param destination endpoint to which the failed offline write was addressed
     * @return whether every candidate failed before any valid first reply arrived
     */
    boolean failed(final InetSocketAddress destination) {
        failed.add(destination);
        return selected == null && failed.size() == candidates.size();
    }

    /**
     * Accepts a first reply from a probed endpoint and rejects all other senders after selection.
     * No application packet or later handshake phase can select a destination.
     * @param datagram borrowed inbound envelope whose reader index and ownership remain unchanged
     * @param config transport codec and offline magic expected by this client
     * @return whether the ordinary handshake pipeline should receive this datagram
     */
    boolean accept(final DatagramPacket datagram, final RakNet.Config config) {
        if (selected != null) {
            return selected.equals(datagram.sender());
        }
        if (!attempted.contains(datagram.sender()) || !validReply(datagram.content(), config)) {
            return false;
        }
        selected = datagram.sender();
        return true;
    }

    /**
     * Decodes only a complete first reply and validates its magic and usable MTU before pinning.
     * @param content borrowed wire data, inspected through an independent reader index
     * @param config negotiated codec contract for the pending connection
     * @return whether the packet is a valid first reply, including when malformed data is received
     */
    private static boolean validReply(final ByteBuf content, final RakNet.Config config) {
        if (content.readableBytes() != 28
                || content.getUnsignedByte(content.readerIndex()) != config.getCodec().packetIdFor(ConnectionReply1.class)) {
            return false;
        }
        final Packet packet;
        try {
            packet = config.getCodec().decode(content.duplicate());
        } catch (IllegalArgumentException | IndexOutOfBoundsException invalid) {
            return false;
        }
        try {
            final ConnectionReply1 reply = (ConnectionReply1) packet;
            reply.getMagic().verify(config.getMagic());
            return reply.getMtu() >= 128 && reply.getMtu() <= 8192;
        } catch (IllegalArgumentException | RakNet.Magic.MagicMismatchException invalid) {
            return false;
        } finally {
            ReferenceCountUtil.release(packet);
        }
    }
}
