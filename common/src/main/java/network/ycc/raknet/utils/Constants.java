package network.ycc.raknet.utils;


/** Retains a legacy packet-loss hook that currently performs no limit enforcement. */
public class Constants {

//    //TODO: get rid of this
//    public static final int MAX_PACKET_LOSS = SystemPropertyUtil
//            .getInt("raknetserver.maxPacketLoss", 8192);

    /**
     * Leaves the supplied loss count unchanged and performs no validation.
     * Callers cannot rely on this legacy hook to bound memory use or packet-processing work.
     *
     * @param n loss or queue count supplied by the caller, currently unused
     * @param location diagnostic description supplied by the caller, currently unused
     */
    public static void packetLossCheck(final int n, final String location) {
//        if (n > Constants.MAX_PACKET_LOSS) {
//            throw new DecoderException("Too big packet loss: " + location);
//        }
    }

}
