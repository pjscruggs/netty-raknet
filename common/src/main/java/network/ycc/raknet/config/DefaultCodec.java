package network.ycc.raknet.config;

import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.AlreadyConnected;
import network.ycc.raknet.packet.ClientHandshake;
import network.ycc.raknet.packet.ConnectionBanned;
import network.ycc.raknet.packet.ConnectionFailed;
import network.ycc.raknet.packet.ConnectionReply1;
import network.ycc.raknet.packet.ConnectionReply2;
import network.ycc.raknet.packet.ConnectionRequest;
import network.ycc.raknet.packet.ConnectionRequest1;
import network.ycc.raknet.packet.ConnectionRequest2;
import network.ycc.raknet.packet.Disconnect;
import network.ycc.raknet.packet.FrameSet;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.packet.InvalidVersion;
import network.ycc.raknet.packet.NoFreeConnections;
import network.ycc.raknet.packet.Packet;
import network.ycc.raknet.packet.Ping;
import network.ycc.raknet.packet.Pong;
import network.ycc.raknet.packet.Reliability;
import network.ycc.raknet.packet.ServerHandshake;
import network.ycc.raknet.packet.SimplePacket;
import network.ycc.raknet.packet.UnconnectedPing;
import network.ycc.raknet.packet.UnconnectedPong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * Registers RakNet packet IDs and converts typed packets to and from their wire representations.
 * Configure a registry before sharing it between channels. Registry maps are mutable and not synchronized.
 * FrameData passed through the framed API is retained, so every returned reference has an explicit owner.
 */
public class DefaultCodec implements RakNet.Codec {

    /** Shared registry initialized with the built-in RakNet packet types. */
    public static final DefaultCodec INSTANCE = new DefaultCodec();

    /** Connected ping carrying the sender timestamp. */
    public static final int PING = 0x00;
    /** Offline discovery request carrying a client identity and clock value. */
    public static final int UNCONNECTED_PING = 0x01;
    /** Connected ping response used for round-trip measurements. */
    public static final int PONG = 0x03;
    /** Offline MTU probe and transport-version proposal. */
    public static final int OPEN_CONNECTION_REQUEST_1 = 0x05;
    /** Server reply accepting the first MTU probe. */
    public static final int OPEN_CONNECTION_REPLY_1 = 0x06;
    /** Offline request binding client identity and negotiated endpoint settings. */
    public static final int OPEN_CONNECTION_REQUEST_2 = 0x07;
    /** Server reply confirming the endpoint and MTU. */
    public static final int OPEN_CONNECTION_REPLY_2 = 0x08;
    /** Reliable request beginning the connected handshake. */
    public static final int CONNECTION_REQUEST = 0x09;
    /** Reserved wire ID for a successful send receipt, not registered by this codec. */
    public static final int SND_RECEIPT_ACKED = 0x0E;
    /** Reserved wire ID for a lost send receipt, not registered by this codec. */
    public static final int SND_RECEIPT_LOSS = 0x0F;
    /** Reliable server handshake carrying endpoint and timing information. */
    public static final int SERVER_HANDSHAKE = 0x10;
    /** Connection-establishment failure response. */
    public static final int CONNECTION_FAILED = 0x11;
    /** Refusal indicating a duplicate connection attempt. */
    public static final int ALREADY_CONNECTED = 0x12;
    /** Client acknowledgement completing the connected handshake. */
    public static final int CLIENT_HANDSHAKE = 0x13;
    /** Refusal indicating exhausted server connection capacity. */
    public static final int NO_FREE_CONNECTIONS = 0x14;
    /** Framed control message requesting connection shutdown. */
    public static final int CLIENT_DISCONNECT = 0x15;
    /** Refusal indicating the peer is banned. */
    public static final int CONNECTION_BANNED = 0x17;
    /** Refusal indicating an unsupported RakNet transport version. */
    public static final int INVALID_VERSION = 0x19;
    /** Offline discovery response advertising server information. */
    public static final int UNCONNECTED_PONG = 0x1C;
    /** First wire ID accepted as a datagram containing framed payloads. */
    public static final int FRAME_DATA_START = 0x80;
    /** Last wire ID accepted as a datagram containing framed payloads. */
    public static final int FRAME_DATA_END = 0x8F;
    /** Negative acknowledgement listing missing datagram ranges. */
    public static final int NACK = 0xA0;
    /** Acknowledgement listing received datagram ranges. */
    public static final int ACK = 0xC0;

    /** Wire-ID lookup for parsers that consume the leading packet ID themselves. */
    protected final Int2ObjectOpenHashMap<Function<ByteBuf, ? extends Packet>> decoders = new Int2ObjectOpenHashMap<>();
    /** Wire-ID lookup for writers that include the packet ID in their output. */
    protected final Int2ObjectOpenHashMap<BiConsumer<? extends Packet, ByteBuf>> encoders = new Int2ObjectOpenHashMap<>();
    /** Exact runtime packet-class lookup, returning minus one for unregistered types. */
    protected final Object2IntOpenHashMap<Class<?>> idFromClass = new Object2IntOpenHashMap<>();
    /** Packet class currently decoded by each wire ID, used to remove stale encoder aliases on replacement. */
    private final Int2ObjectOpenHashMap<Class<?>> classesById = new Int2ObjectOpenHashMap<>();
    /** Registered IDs whose decoded types can carry framed reliability and ordering metadata. */
    protected final IntOpenHashSet framedPacketIds = new IntOpenHashSet();

    /**
     * Registers built-in control packets and every recognized frame-set ID, then marks unknown classes with minus one.
     */
    public DefaultCodec() {
        //       ID                         Class                       Decoder                     Encoder
        registerBuiltin(PING,                      Ping.class,                 Ping::new);
        registerBuiltin(UNCONNECTED_PING,          UnconnectedPing.class,      UnconnectedPing::new);
        registerBuiltin(PONG,                      Pong.class,                 Pong::new);
        registerBuiltin(OPEN_CONNECTION_REQUEST_1, ConnectionRequest1.class,   ConnectionRequest1::new);
        registerBuiltin(OPEN_CONNECTION_REPLY_1,   ConnectionReply1.class,     ConnectionReply1::new);
        registerBuiltin(OPEN_CONNECTION_REQUEST_2, ConnectionRequest2.class,   ConnectionRequest2::new);
        registerBuiltin(OPEN_CONNECTION_REPLY_2,   ConnectionReply2.class,     ConnectionReply2::new);
        registerBuiltin(CONNECTION_REQUEST,        ConnectionRequest.class,    ConnectionRequest::new);
        registerBuiltin(SERVER_HANDSHAKE,          ServerHandshake.class,      ServerHandshake::new);
        registerBuiltin(CONNECTION_FAILED,         ConnectionFailed.class,     ConnectionFailed::new);
        registerBuiltin(ALREADY_CONNECTED,         AlreadyConnected.class,     AlreadyConnected::new);
        registerBuiltin(CLIENT_HANDSHAKE,          ClientHandshake.class,      ClientHandshake::new);
        registerBuiltin(NO_FREE_CONNECTIONS,       NoFreeConnections.class,    NoFreeConnections::new);
        registerBuiltin(CLIENT_DISCONNECT,         Disconnect.class,           Disconnect::new);
        registerBuiltin(CONNECTION_BANNED,         ConnectionBanned.class,     ConnectionBanned::new);
        registerBuiltin(INVALID_VERSION,           InvalidVersion.class,       InvalidVersion::new);
        registerBuiltin(UNCONNECTED_PONG,          UnconnectedPong.class,      UnconnectedPong::new);
        for (int i = FRAME_DATA_START; i <= FRAME_DATA_END; i++) {
            installRegistration(i, FrameSet.class, FrameSet::read, FrameSet::write);
        }
        registerBuiltin(NACK,                      Reliability.NACK.class,     Reliability.NACK::new);
        registerBuiltin(ACK,                       Reliability.ACK.class,      Reliability.ACK::new);

        idFromClass.defaultReturnValue(-1);
    }

    /**
     * Encodes a framed packet and copies its reliability and channel into the resulting frame data.
     * Existing FrameData is returned with one additional reference instead of being serialized again.
     *
     * @param packet typed framed packet or live FrameData whose ownership stays with the caller
     * @param alloc allocator used when a typed packet needs serialization
     * @return caller-owned FrameData reference that must be released or forwarded
     */
    @Override
    public FrameData encode(final FramedPacket packet, final ByteBufAllocator alloc) {
        if (packet instanceof FrameData) {
            return ((FrameData) packet).retain();
        }
        final ByteBuf out = alloc.ioBuffer(packet.sizeHint());
        try {
            encode(packet, out);
            final FrameData frameData = FrameData.read(out, out.readableBytes(), false);
            frameData.setReliability(packet.getReliability());
            frameData.setOrderChannel(packet.getOrderChannel());
            return frameData;
        } finally {
            out.release();
        }
    }

    /**
     * Appends a registered packet, including its wire ID, using the exact runtime class lookup.
     *
     * @param packet registered packet whose ownership remains with the caller
     * @param out caller-owned destination whose writer index advances
     * @throws IllegalArgumentException if the exact packet class has no registered encoder
     */
    @SuppressWarnings("unchecked")
    @Override
    public void encode(final Packet packet, final ByteBuf out) {
        if (!idFromClass.containsKey(packet.getClass())) {
            throw new IllegalArgumentException("Unknown encoder for " + packet.getClass());
        }
        final int packetId = packetIdFor(packet.getClass());
        final BiConsumer<Packet, ByteBuf> encoder = (BiConsumer<Packet, ByteBuf>) encoders
                .get(packetId);
        encoder.accept(packet, out);
    }

    /**
     * Produces a caller-owned wire buffer without consuming the input packet reference.
     * Frame sets of at least 128 rough bytes use composite payload buffers, while other packets are copied.
     *
     * @param packet packet whose encoding or retained payload views remain valid during this call
     * @param alloc allocator for the encoded buffer
     * @return encoded datagram buffer that the caller must release or forward
     */
    @Override
    public ByteBuf produceEncoded(final Packet packet, final ByteBufAllocator alloc) {
        if (packet instanceof FrameSet && ((FrameSet) packet).getRoughSize() >= 128) {
            return ((FrameSet) packet).produce(alloc);
        }
        final ByteBuf buf = alloc.ioBuffer(packet.sizeHint());
        try {
            encode(packet, buf);
            return buf.retain();
        } finally {
            buf.release();
        }
    }

    /**
     * Selects a parser from the leading unsigned packet ID and lets it consume the message.
     *
     * @param buf caller-owned readable source including the packet ID
     * @return decoded packet, which may own retained input slices
     * @throws IllegalArgumentException if the leading packet ID has no registered decoder
     */
    @Override
    public Packet decode(final ByteBuf buf) {
        final int packetId = buf.getUnsignedByte(buf.readerIndex());
        final Function<ByteBuf, ? extends Packet> decoder = decoders.get(packetId);
        if (decoder == null) {
            throw new IllegalArgumentException("Unknown decoder for packet ID " + packetId);
        }
        return decoder.apply(buf);
    }

    /**
     * Decodes recognized framed control packets while preserving their transport metadata.
     * Unregistered or non-framed IDs remain opaque FrameData and are returned with one additional reference.
     *
     * @param data caller-owned complete frame data including its packet ID
     * @return decoded control packet or a newly retained reference to the original FrameData
     */
    @Override
    public FramedPacket decode(final FrameData data) {
        final int packetId = data.getPacketId();
        final Function<ByteBuf, ? extends Packet> decoder = decoders.get(packetId);
        if (decoder == null || !framedPacketIds.contains(packetId)) {
            return data.retain();
        }
        final ByteBuf buf = data.createData();
        try {
            final FramedPacket out = (FramedPacket) decoder.apply(buf);
            out.setReliability(data.getReliability());
            out.setOrderChannel(data.getOrderChannel());
            return out;
        } finally {
            buf.release();
        }
    }

    /**
     * Looks up the encoder ID for an exact packet class, without searching supertypes.
     *
     * @param type runtime packet class to query
     * @return registered wire ID, or minus one if the class is unknown
     */
    @Override
    public int packetIdFor(final Class<? extends Packet> type) {
        return idFromClass.getInt(type);
    }

    /**
     * Installs a built-in packet without dispatching to subclass hooks before subclass initialization.
     * Subclasses may customize registrations after their own fields have been initialized.
     *
     * @param <T> built-in simple packet type
     * @param id packet's one-byte wire identifier
     * @param type exact packet class used for encoding
     * @param constructor factory invoked only when decoding a packet
     */
    private <T extends SimplePacket> void registerBuiltin(final int id, final Class<T> type,
            final Supplier<T> constructor) {
        installRegistration(id, type, simpleDecoder(constructor), simpleEncoder(id));
    }

    ///// REGISTRY /////

    /**
     * Registers a simple packet using adapters that write and consume its leading wire ID.
     *
     * @param <T> simple packet type created by the constructor supplier
     * @param id wire packet ID to register
     * @param clz exact packet class used for encoder lookup
     * @param cons supplier of an uninitialized packet for each decode
     */
    protected <T extends SimplePacket> void register(final int id, final Class<T> clz, final Supplier<T> cons) {
        register(id, clz, decodeSimple(cons), encodeSimple(id));
    }

    /**
     * Installs class and wire-ID mappings, marking framed packet classes for framed decoding.
     * Existing mappings for the supplied keys are replaced. Register only before concurrent channel use.
     *
     * @param <T> packet type accepted by the encoder and returned by the decoder
     * @param id wire packet ID shared by the supplied parser and writer
     * @param clz exact packet class used for encoder lookup
     * @param decoder parser that consumes the packet ID and body
     * @param encoder writer that emits the packet ID and body
     */
    protected <T extends Packet> void register(final int id, final Class<? extends Packet> clz,
            final Function<ByteBuf, T> decoder, final BiConsumer<T, ByteBuf> encoder) {
        installRegistration(id, clz, decoder, encoder);
    }

    /**
     * Replaces a wire-ID registration and removes aliases that would otherwise select the replacement's wrong encoder.
     * Multiple IDs may still decode one class, as frame sets require, while encoding uses its latest registration.
     *
     * @param <T> packet type accepted by the parser and writer
     * @param id wire identifier whose registration is replaced
     * @param clz exact packet class associated with the identifier
     * @param decoder parser consuming the wire identifier and body
     * @param encoder writer emitting the wire identifier and body
     */
    private <T extends Packet> void installRegistration(final int id, final Class<? extends Packet> clz,
            final Function<ByteBuf, T> decoder, final BiConsumer<T, ByteBuf> encoder) {
        final Class<?> previous = classesById.put(id, clz);
        if (previous != null && idFromClass.getInt(previous) == id) {
            idFromClass.removeInt(previous);
        }
        idFromClass.put(clz, id);
        decoders.put(id, decoder);
        encoders.put(id, encoder);
        if (FramedPacket.class.isAssignableFrom(clz)) {
            framedPacketIds.add(id);
        } else {
            framedPacketIds.remove(id);
        }
    }

    /**
     * Builds a decoder that creates a fresh packet, consumes its one-byte ID and reads its body.
     *
     * @param <T> simple packet type produced by the supplier
     * @param cons supplier invoked once for each decoded message
     * @return body-decoder adapter that leaves buffer ownership with its caller
     */
    protected <T extends SimplePacket> Function<ByteBuf, T> decodeSimple(final Supplier<T> cons) {
        return simpleDecoder(cons);
    }

    /**
     * Creates a simple packet reader without invoking overridable methods during registry construction.
     *
     * @param <T> packet type produced by the supplied factory
     * @param cons factory used once for each decoded body
     * @return decoder that borrows its input buffer without retaining or releasing it
     */
    private static <T extends SimplePacket> Function<ByteBuf, T> simpleDecoder(final Supplier<T> cons) {
        return buf -> {
            final T inst = cons.get();
            buf.skipBytes(1);
            inst.decode(buf);
            return inst;
        };
    }

    /**
     * Builds an encoder that writes the chosen one-byte ID before delegating to the packet body writer.
     *
     * @param <T> simple packet type accepted by the writer
     * @param id packet ID encoded before each body
     * @return encoder adapter that leaves packet and buffer ownership with its caller
     */
    protected <T extends SimplePacket> BiConsumer<T, ByteBuf> encodeSimple(final int id) {
        return simpleEncoder(id);
    }

    /**
     * Creates a simple packet writer without invoking overridable methods during registry construction.
     *
     * @param <T> packet type whose body encoder receives the destination buffer
     * @param id one-byte identifier written before the packet body
     * @return writer preserving packet and destination ownership
     */
    private static <T extends SimplePacket> BiConsumer<T, ByteBuf> simpleEncoder(final int id) {
        return (packet, buf) -> {
            buf.writeByte(id);
            packet.encode(buf);
        };
    }

}
