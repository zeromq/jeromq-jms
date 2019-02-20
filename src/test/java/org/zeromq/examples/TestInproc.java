
package org.zeromq.examples;

import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;

/**
 * Test straight JeroMQ "inproc" sockets without the JMS wrapper.
 */
public class TestInproc {
    private static final Logger LOGGER = Logger.getLogger(TestInproc.class.getCanonicalName());

    /**
     * Test Push/Pull 'inproc" socket.
     */
    @Test
    public void tesdPushPullInproc() {
        final ZMQ.Context context = ZMQ.context(1);

        final ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        final boolean rc = sender.bind("inproc://test");

        Assert.assertTrue(rc);

        final ZMQ.Socket reciever = context.socket(ZMQ.PULL);
        reciever.connect("inproc://test");
        sender.send("This is message 1", 0);

        final String message = reciever.recvStr();

        LOGGER.info("Message recieve: " + message);

        Assert.assertNotNull(message);

        sender.close();
        reciever.close();
    }
}
