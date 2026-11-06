package network.ycc.raknet;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import network.ycc.raknet.channel.DatagramChannelProxy;
import network.ycc.raknet.client.channel.RakNetClientThreadedChannel;
import network.ycc.raknet.config.DefaultCodec;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.packet.Ping;
import network.ycc.raknet.pipeline.UserDataCodec;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import network.ycc.raknet.utils.EmptyInit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/** Checks socket lifecycle and reliable payload delivery through seeded datagram loss and reordering. */
@Timeout(120)
class EndToEndTest {
    /** Owns client and server UDP socket processing for each fixture instance. */
    final EventLoopGroup ioGroup = new NioEventLoopGroup(2);
    /** Owns server child connections and serializes the fixture's write-promise aggregation. */
    final EventLoopGroup childGroup = new NioEventLoopGroup(2);
    /** Every real socket created by this fixture, retained only to verify close completion. */
    final Queue<NioDatagramChannel> sockets = new ConcurrentLinkedQueue<>();
    /** Loopback endpoint used by the real UDP client and server in these sequential scenarios. */
    final InetSocketAddress localhost = new InetSocketAddress("localhost", 31745);

    /**
     * Installs a caller-supplied inbound consumer and schedules a small framed control payload.
     * The consumer owns each incoming reference and must forward or release it.
     *
     * @param func consumer invoked for each decoded inbound message
     * @return initializer used by the test server's accepted child channels
     */
    public static ChannelInitializer<Channel> simpleHandler(
            final BiConsumer<ChannelHandlerContext, Object> func) {
        return new ChannelInitializer<Channel>() {
            /**
             * Adds the test consumer and schedules one write on the accepted channel's event loop.
             * @param ch server child whose inbound traffic is being observed
             */
            @Override
            protected void initChannel(final Channel ch) {
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    /**
                     * Transfers the inbound reference to the fixture's explicit ownership-aware consumer.
                     * @param ctx server application's inbound context
                     * @param msg message now owned by the configured consumer
                     */
                    @Override
                    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                        func.accept(ctx, msg);
                    }
                });
                ch.eventLoop().schedule(() -> {
                    final ByteBuf buf1 = ch.alloc().buffer(1).writeByte(0);
                    ch.write(FrameData.create(ch.alloc(), 0xF0, buf1));
                    buf1.release();
                }, 10, TimeUnit.MILLISECONDS);
            }
        };
    }

    /** Stops both fixture-owned event-loop groups even when setup or an assertion fails. */
    @AfterEach
    public void stopEventLoops() {
        ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        childGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        ioGroup.terminationFuture().syncUninterruptibly();
        childGroup.terminationFuture().syncUninterruptibly();
    }

    /**
     * Repeatedly binds and closes the listener to detect sockets left bound after shutdown.
     * @throws Throwable if any bind or close operation fails
     */
    @Test
    void serverCloseTest() throws Throwable {
        for (int i = 0; i < 20; i++) {
            newServer(null, null).close().sync();
        }
    }

    /**
     * Repeats handshakes with server-initiated shutdown before closing the client side.
     * @throws Throwable if connection establishment or ordered shutdown fails
     */
    @Test
    void connectAndCloseTestServerFirst() throws Throwable {
        for (int i = 0; i < 20; i++) {
            Channel server = newServer(null, null);
            Channel client = newClient(null);

            closeWithinDeadline(server);
            closeWithinDeadline(client);
            sockets.forEach(socket -> Assertions.assertFalse(socket.isOpen(), "Socket remains open after peer close"));
        }
    }

    /**
     * Repeats handshakes with client-initiated shutdown before closing the listening server.
     * @throws Throwable if connection establishment or ordered shutdown fails
     */
    @Test
    void connectAndCloseTestClientFirst() throws Throwable {
        for (int i = 0; i < 20; i++) {
            Channel server = newServer(null, null);
            Channel client = newClient(null);

            client.close().sync();
            server.close().sync();
        }
    }

    /**
     * Delivers one application buffer after injecting a malformed transport message into the client.
     * @throws Throwable if the connection, delivery or channel cleanup fails
     */
    @Test
    void singleBufferTest() throws Throwable {
        int bytesSent = 1000;
        AtomicInteger bytesRecvd = new AtomicInteger(0);

        Channel server = newServer(null, simpleHandler((ctx, msg) -> {
            if (msg instanceof ByteBuf) {
                bytesRecvd.addAndGet(((ByteBuf) msg).readableBytes());
            }
            ReferenceCountUtil.safeRelease(msg);
        }));
        Channel client = newClient(null);

        //add some bad frame data, should be ignore safely
        client.parent().pipeline().fireChannelRead(Unpooled.wrappedBuffer(
                new byte[]{(byte) DefaultCodec.FRAME_DATA_START, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

        client.pipeline().write(Unpooled.wrappedBuffer(new byte[bytesSent]));
        client.pipeline().flush();

        Thread.sleep(1000); //give pings time to run

        server.close().sync();
        client.close().sync();

        Assertions.assertEquals(bytesSent, bytesRecvd.get());
    }

    /**
     * Sends seeded bulk payloads up to 5,000 bytes, exercising fragmentation without injected loss.
     * @throws Throwable if setup, reliable delivery or cleanup fails
     */
    @Test
    void manyBufferTest() throws Throwable {
        dataTest(100, 5000, false, false);
    }

    /**
     * Recovers reliable payloads from outgoing datagram loss, duplication and reordering.
     * @throws Throwable if setup, reliable delivery or cleanup fails
     */
    @Test
    void manyBufferBadClient() throws Throwable {
        dataTest(100, 1000, true, false);
    }

    /**
     * Recovers reliable payloads when incoming acknowledgements are dropped, duplicated or reordered.
     * @throws Throwable if reliable delivery does not recover from the seeded faults
     */
    @Test
    void manyBufferBadServer() throws Throwable {
        dataTest(100, 1000, false, true);
    }

    /**
     * Combines fragmented bulk messages with seeded faults on incoming and outgoing client datagrams.
     * @throws Throwable if reassembly or reliable delivery fails under the injected faults
     */
    @Test
    void manyBufferBadBoth() throws Throwable {
        dataTest(100, 5000, true, true);
    }


    /**
     * Sends seeded payloads through a loopback UDP path and verifies reliable delivery.
     * Eight-byte payloads identify individual reliable messages, while other sizes count bulk bytes.
     *
     * @param nSend number of bulk payloads to send
     * @param maxSize upper bound of the randomly selected bulk payload size
     * @param brutalizeWrite whether to inject loss, duplication and reordering on outgoing datagrams
     * @param brutalizeRead whether to inject the same faults on incoming datagrams
     * @throws Throwable if connection setup, delivery confirmation or cleanup fails
     */
    public void dataTest(final int nSend, final int maxSize, final boolean brutalizeWrite,
            final boolean brutalizeRead) throws Throwable {
        Random rnd = new Random(34598);
        AtomicInteger bytesSent = new AtomicInteger(0);
        AtomicInteger bytesRecvd = new AtomicInteger(0);
        AtomicInteger numRecvd = new AtomicInteger(0);
        AtomicInteger pending = new AtomicInteger(0);
        ConcurrentMap<Long, Object> reliableSet = new ConcurrentHashMap<>();
        EventLoop testLoop = childGroup.next();
        PromiseCombiner combiner = new PromiseCombiner();
        Brutalizer brutalizer = new Brutalizer();

        Channel server = newServer(null, simpleHandler((ctx, msg) ->
                recordDelivery(msg, reliableSet, numRecvd, bytesRecvd)));
        Channel client = newClient(null);
        ChannelPromise donePromise = client.newPromise();

        client.parent().pipeline()
                .addAfter(DatagramChannelProxy.LISTENER_HANDLER_NAME, "brutalizer", brutalizer);
        brutalizer.rnd = rnd;
        brutalizer.brutalizeRead = brutalizeRead;
        brutalizer.brutalizeWrite = brutalizeWrite;

        //TODO: server side writes?

        for (int i = 0; i < nSend; i++) {
            int size = rnd.nextInt(maxSize) + 1;
            if (size == 8) {
                size = 9; //reserve 8 size for the other tests
            }
            awaitWritable(client, pending);
            ChannelFuture fut;
            switch (rnd.nextInt(2)) {
                case 0:
                    fut = client.pipeline().write(Unpooled.wrappedBuffer(new byte[size]));
                    break;
                default:
                    FrameData data = createPayload(client, Unpooled.wrappedBuffer(new byte[size]));
                    if (rnd.nextBoolean()) {
                        data.setReliability(FramedPacket.Reliability.RELIABLE_ORDERED);
                        data.setOrderChannel(rnd.nextInt(4));
                    }
                    fut = client.pipeline().write(data);
            }
            testLoop.execute(() -> combiner.add(fut));
            bytesSent.addAndGet(size);
            pending.incrementAndGet();
            fut.addListener(x -> pending.decrementAndGet());
        }

        for (int i = 0; i < 200; i++) {
            long value = rnd.nextLong();
            reliableSet.put(value, true);
            ByteBuf buf = Unpooled.wrappedBuffer(new byte[8]);
            buf.clear();
            buf.writeLong(value);
            FrameData packet = createPayload(client, buf);
            packet.setReliability(FramedPacket.Reliability.RELIABLE);
            testLoop.execute(() -> combiner.add(client.pipeline().write(packet)));
        }

        client.pipeline().flush();
        client.write(new Ping()).sync();

        testLoop.execute(() -> combiner.finish(donePromise));

        //TODO: new test loop for UNRELIABLE + RELIABLE

        try {
            donePromise.get(90, TimeUnit.SECONDS);
        } finally {
            server.close().sync();
            client.close().sync();
        }
        Assertions.assertTrue(reliableSet.isEmpty());
        Assertions.assertEquals(0, pending.get());
        Assertions.assertEquals(nSend, numRecvd.get());
        Assertions.assertEquals(bytesSent.get(), bytesRecvd.get());
    }

    /**
     * Wraps a fixture payload in a frame and releases the original reference retained by the factory.
     *
     * @param channel connection supplying the frame allocator
     * @param payload input reference consumed even if frame creation fails
     * @return frame owning the remaining payload reference
     */
    private static FrameData createPayload(final Channel channel, final ByteBuf payload) {
        try {
            return FrameData.create(channel.alloc(), 0xFE, payload);
        } finally {
            payload.release();
        }
    }

    /**
     * Waits for transport backpressure to clear without spinning forever after a failed connection.
     *
     * @param client channel that must regain outbound capacity
     * @param pending outstanding fixture write promises
     * @throws AssertionError if the channel stays blocked for thirty seconds or the test is interrupted
     */
    private static void awaitWritable(final Channel client, final AtomicInteger pending) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!client.isWritable() || pending.get() > 3000) {
            if (System.nanoTime() - deadline >= 0 || Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Timed out waiting for transport write capacity");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    /**
     * Separates reliable message identities from bulk delivery counters and releases every input.
     *
     * @param msg inbound payload or transport notification owned by this callback
     * @param reliableSet identities still awaiting reliable delivery
     * @param numRecvd number of received bulk payloads
     * @param bytesRecvd total bulk payload bytes received
     */
    private static void recordDelivery(final Object msg,
            final ConcurrentMap<Long, Object> reliableSet, final AtomicInteger numRecvd,
            final AtomicInteger bytesRecvd) {
        try {
            if (msg instanceof ByteBuf) {
                final ByteBuf buf = (ByteBuf) msg;
                if (buf.readableBytes() == Long.BYTES) {
                    reliableSet.remove(buf.readLong());
                } else {
                    numRecvd.incrementAndGet();
                    bytesRecvd.addAndGet(buf.readableBytes());
                }
            }
        } finally {
            ReferenceCountUtil.safeRelease(msg);
        }
    }

    /**
     * Requires a close request to finish promptly and reports both wrapper and transport state on failure.
     *
     * @param channel application or listening channel whose close contract is under test
     * @throws InterruptedException if the bounded wait is interrupted
     */
    private static void closeWithinDeadline(final Channel channel) throws InterruptedException {
        final ChannelFuture closing = channel.close();
        Assertions.assertTrue(closing.await(5, TimeUnit.SECONDS),
                () -> "Close incomplete: open=" + channel.isOpen() + ", registered=" + channel.isRegistered()
                        + ", pipeline=" + channel.pipeline().names() + ", parent=" + channel.parent());
        Assertions.assertTrue(closing.isSuccess(), () -> String.valueOf(closing.cause()));
    }

    /**
     * Creates and records a real UDP socket so shutdown checks include unregistered handshake sockets.
     *
     * @return fresh socket whose lifetime belongs to the transport under test
     */
    private NioDatagramChannel newDatagramChannel() {
        final NioDatagramChannel socket = new NioDatagramChannel();
        sockets.add(socket);
        return socket;
    }

    /**
     * Binds a real UDP RakNet listener with application packet decoding on every accepted child.
     *
     * @param ioInit optional listener-pipeline initializer, replaced with an empty initializer when null
     * @param childInit optional application handler installed after the test payload codec
     * @return bound server channel that the caller must close
     * @throws InterruptedException if the bind-completion wait is interrupted
     */
    public Channel newServer(final ChannelInitializer<Channel> ioInit,
            final ChannelInitializer<Channel> childInit)
            throws InterruptedException {
        final ChannelInitializer<Channel> effectiveIoInit = ioInit == null ? new EmptyInit() : ioInit;
        final ServerBootstrap bootstrap = new ServerBootstrap()
                .group(ioGroup, childGroup)
                .channelFactory(() -> new RakNetServerChannel(this::newDatagramChannel))
                .option(RakNet.SERVER_ID, 12345L)
                .option(RakNet.RETRY_DELAY_NANOS,
                        TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS))
                .handler(effectiveIoInit)
                .childHandler(new ChannelInitializer<Channel>() {
                    /**
                     * Decodes the test payload identifier before handing messages to the optional consumer.
                     * @param ch newly accepted server-side connection
                     */
                    @Override
                    protected void initChannel(final Channel ch) {
                        ch.pipeline().addLast(UserDataCodec.NAME, new UserDataCodec(0xFE));
                        if (childInit != null) {
                            ch.pipeline().addLast(childInit);
                        }
                    }
                });
        return bootstrap.bind(localhost).sync().channel();
    }

    /**
     * Establishes a threaded RakNet client against the fixture's real UDP listener.
     *
     * @param init optional application handler installed after the matching payload codec
     * @return connected client channel that the caller must close
     * @throws InterruptedException if the connection-completion wait is interrupted
     */
    public Channel newClient(final ChannelInitializer<Channel> init)
            throws InterruptedException {
        final Bootstrap bootstrap = new Bootstrap()
                .group(ioGroup)
                .channelFactory(() -> new RakNetClientThreadedChannel(this::newDatagramChannel))
                .option(RakNet.CLIENT_ID, 6789L)
                .option(RakNet.RETRY_DELAY_NANOS,
                        TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS))
                .handler(new ChannelInitializer<Channel>() {
                    /**
                     * Uses the server's application packet identifier before installing optional client behavior.
                     * @param ch client application channel initialized by Bootstrap
                     */
                    @Override
                    protected void initChannel(final Channel ch) {
                        ch.pipeline().addLast(UserDataCodec.NAME, new UserDataCodec(0xFE));
                        if (init != null) {
                            ch.pipeline().addLast(init);
                        }
                    }
                });
        return bootstrap.connect(localhost).sync().channel();
    }

    /** Injects reproducible drops, duplicates and delayed datagrams on the client's socket pipeline. */
    public static class Brutalizer extends ChannelDuplexHandler {
        /** Seeded source shared with the payload generator to reproduce a complete fault sequence. */
        Random rnd;
        /** Whether outgoing datagrams are subject to the configured fault probabilities. */
        boolean brutalizeWrite;
        /** Whether incoming datagrams are subject to the configured fault probabilities. */
        boolean brutalizeRead;
        /** Probability that a datagram is dropped or saved for delayed delivery. */
        double lossPercent = 0.20;
        /** Probability of forwarding an additional retained reference to the same datagram. */
        double dupePercent = 0.20;
        /** Probability used to save or release a delayed datagram on a subsequent operation. */
        double orderPercent = 0.20;

        /** Outgoing datagram reference owned by the injector until forwarded or released. */
        Object writeStash;
        /** Incoming datagram reference owned by the injector until forwarded or released. */
        Object readStash;

        /**
         * Releases any delayed datagrams still owned by the injector when its pipeline is removed.
         * @param ctx removed injector's context
         */
        @Override
        public void handlerRemoved(final ChannelHandlerContext ctx) {
            ReferenceCountUtil.safeRelease(writeStash);
            ReferenceCountUtil.safeRelease(readStash);
        }

        /**
         * Drops, delays or duplicates outgoing datagrams and completes dropped writes as UDP loss.
         * @param ctx outbound socket context used for forwarding surviving datagrams
         * @param msg outgoing reference consumed, retained for delay or transferred downstream
         * @param promise write completion, resolved successfully even for simulated packet loss
         * @throws Exception if an ordinary downstream write fails synchronously
         */
        @Override
        public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
                throws Exception {
            if (brutalizeWrite) {
                if (rnd.nextDouble() < orderPercent && writeStash != null) {
                    ctx.write(writeStash);
                    writeStash = null;
                }
                if (rnd.nextDouble() < lossPercent) {
                    if (rnd.nextDouble() < orderPercent) {
                        ReferenceCountUtil.safeRelease(writeStash);
                        writeStash = msg;
                    } else {
                        ReferenceCountUtil.release(msg);
                    }
                    promise.trySuccess();
                    return;
                }
                if (rnd.nextDouble() < dupePercent) {
                    super.write(ctx, ReferenceCountUtil.retain(msg), ctx.voidPromise());
                }
            }
            super.write(ctx, msg, promise);
        }

        /**
         * Applies the seeded inbound loss and duplication decisions while retaining delayed inputs.
         * @param ctx inbound socket context receiving datagrams from the peer
         * @param msg incoming reference consumed, saved or forwarded by the injector
         * @throws Exception if a downstream inbound handler fails synchronously
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
            if (brutalizeRead) {
                if (rnd.nextDouble() < orderPercent && readStash != null) {
                    ctx.fireChannelRead(readStash);
                    readStash = null;
                }
                if (rnd.nextDouble() < lossPercent) {
                    if (rnd.nextDouble() < orderPercent) {
                        ReferenceCountUtil.safeRelease(readStash);
                        readStash = msg;
                    } else {
                        ReferenceCountUtil.release(msg);
                    }
                    return;
                }
                if (rnd.nextDouble() < dupePercent) {
                    super.channelRead(ctx, ReferenceCountUtil.retain(msg));
                }
            }
            super.channelRead(ctx, msg);
        }
    }

}
