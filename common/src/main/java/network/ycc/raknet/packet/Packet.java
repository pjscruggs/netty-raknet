package network.ycc.raknet.packet;

/** Marks messages handled by the transport codec, including offline packets and reference-counted frame sets. */
public interface Packet {

    /**
     * Suggests an initial encoding capacity rather than an exact wire size or an upper bound.
     *
     * @return default allocation hint of 128 bytes
     */
    default int sizeHint() {
        return 128;
    }

    /** Identifies handshake messages that carry the client identity used to detect a restarted session. */
    @FunctionalInterface
    interface ClientIdConnection extends Packet {
        /**
         * Returns the client identity announced in this handshake message.
         *
         * @return opaque 64-bit identity compared across connection-establishment packets
         */
        long getClientId();
    }

}
