package org.zeromq.examples;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

/**
 * Test the setup required for a bind sockets after a unbind (limit use a unit test, more to test interprocess.
 *
 * NOTE:
 * This only works with 2 UNIT TEST running since REUSE can only work on separate processes. Unix TCP sockets do
 * have ability to set an option SO_REUSEADDR, but this is not available in ZMQ. However, setting Linger to 0, other
 * process can bind, which is all the requirement for the JeroMQ-JMS DR/BCP functionality.
 */
public class TestBindExamples {

    private static final Logger LOGGER = Logger.getLogger(TestBindExamples.class.getCanonicalName());

    /**
     * Test the setup of binding to a socket that has been release.
     */
    @Test
    public void testBindUnbondBind() {
        final String addr = "tcp://*:9550";
        final ZMQ.Context context1 = ZMQ.context(1);
        final ZMQ.Context context2 = ZMQ.context(1);

        LOGGER.info("Sicekt1: Bind to addr: " + addr);
        final ZMQ.Socket socket1 = context1.socket(ZMQ.ROUTER);
        final ZMQ.Socket socket2 = context2.socket(ZMQ.ROUTER);

        socket1.setLinger(0);
        //socket1.setReuseAddress(true);
        socket1.bind(addr);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "Sleep was iterrupted", ex);
        }

        try {
            socket2.bind(addr);
        } catch (ZMQException ex) {
            LOGGER.info("Socket2: Cannot bind to addr: " + addr);
        }

        LOGGER.info("Socket1: Unbind from addr: " + addr);
        boolean returnStatus = socket1.unbind(addr);
        LOGGER.info("Socket1: Unbind status: " + returnStatus);
        socket1.close();
        context1.close();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "Sleep was iterrupted", ex);
        }

        try {
            socket2.bind(addr);

            LOGGER.warning("Socket2: Must be running 2 process, unless new ZMQ functionality.");
        } catch (ZMQException ex) {
            LOGGER.info("Socket2: Current ZMQ expect behaviour, cannot bind to addr: " + addr);
        }

        returnStatus = socket2.unbind(addr);
        LOGGER.info("Socket2: Unbind status: " + returnStatus);

        socket2.close();
        context2.close();
        LOGGER.info("Test finished!");
    }

}
