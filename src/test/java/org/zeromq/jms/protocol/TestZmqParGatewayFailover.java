package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.logging.Logger;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqTextMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;
import org.zeromq.jms.protocol.ZmqGateway.Direction;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.event.ZmqStompEventHandler;
import org.zeromq.jms.util.Stopwatch;

/**
 * Test for PAR gateway fail-over protocol functionality.
 */
public class TestZmqParGatewayFailover {

    private static final Logger LOGGER = Logger.getLogger(TestZmqParGatewayFailover.class.getCanonicalName());

    private static final String SOCKET_ADDR_1 = "tcp://*:9735";
    private static final String SOCKET_ADDR_2 = "tcp://*:9736";

    private static final String SOCKET_ADDR_3 = "tcp://*:9739";
    private static final String SOCKET_ADDR_4 = "tcp://*:9740";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";

    /**
     * Test a send and receive protocol functionality were a re-direct on fail-over.
     * @throws InterruptedException  throws interrupt exception on failed waits
     */
    @Test
    public void testFailoverWithRedirect() throws InterruptedException {
        LOGGER.info("Start Failover with Redirect test.");

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqGateway sender =
        	new ZmqParGateway("protocol:sender1", context, ZmqSocketType.DEALER, true, SOCKET_ADDR_1 + "," + SOCKET_ADDR_2,
                flags, null, handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqGateway receiver1 =
        	new ZmqParGateway("protocol:receiver1", context, ZmqSocketType.ROUTER, false, SOCKET_ADDR_1, flags, null, handler, 
                null, null, null, null, false, Direction.INCOMING);

        final ZmqGateway receiver2 =
        	new ZmqParGateway("protocol:receiver2", context, ZmqSocketType.ROUTER, false, SOCKET_ADDR_2, flags, null, handler,
                null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();

            sender.open();

            receiver1.open();
            receiver2.open();

            try {
                Stopwatch stopwatch = new Stopwatch();

                sender.send(outMessage1);

                ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver1.receive(1000);

                if (inMessage1 == null) {
                    inMessage1 = (ZmqTextMessage) receiver2.receive(1000);
                    receiver2.close();
                } else {
                    receiver1.close();
                }

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                LOGGER.info("Elapse time (msec): " + stopwatch.elapsedTime());

                sender.send(outMessage2);

                ZmqTextMessage inMessage2 = null;

                if (receiver1.isActive()) {
                    inMessage2 = (ZmqTextMessage) receiver1.receive(1000);
                } else {
                    inMessage2 = (ZmqTextMessage) receiver2.receive(1000);
                }

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());
            } catch (ZmqException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                LOGGER.info("Closing sender gateway");

                sender.close();

                LOGGER.info("Closing receiver gateway");

                if (receiver1.isActive()) {
                    receiver1.close();
                }
                if (receiver2.isActive()) {
                    receiver2.close();
                }

                Thread.sleep(1000);

                LOGGER.info("Closing context");

                context.close();
            }
        } catch (JMSException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }

        LOGGER.info("Finish Failover with Redirect test.");
    }

    /**
     * Test a send and receive protocol functionality with a restart.
     * @throws InterruptedException  throws interrupt exception on failed waits
     */
    @Test
    public void testFailoverWithRestart() throws InterruptedException {
        LOGGER.info("Start Failover with Restart test.");

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqGateway sender =
        	new ZmqParGateway("protocol:sender2", context, ZmqSocketType.DEALER, true, SOCKET_ADDR_3 + "," + SOCKET_ADDR_4,
                flags, null, handler,
                null, null, null, null, false, Direction.OUTGOING);

        final ZmqGateway receiver1 =
        	new ZmqParGateway("protocol:receiver3", context, ZmqSocketType.ROUTER, false, SOCKET_ADDR_3,
        		flags, null, handler,
                null, null, null, null, false, Direction.INCOMING);

        final ZmqGateway receiver2 = new ZmqParGateway("protocol:receiver4", context, ZmqSocketType.ROUTER, false, SOCKET_ADDR_4,
        		flags, null, handler,
        		null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();

            sender.open();

            receiver1.open();
            receiver2.open();

            try {
                Stopwatch stopwatch = new Stopwatch();

                sender.send(outMessage1);

                ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver1.receive(300);

                if (inMessage1 == null) {
                    inMessage1 = (ZmqTextMessage) receiver2.receive(300);
                }

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                LOGGER.info("Elapse time (msec): " + stopwatch.elapsedTime());

                receiver1.close();
                receiver2.close();

                stopwatch = new Stopwatch();

                sender.send(outMessage2);

                receiver1.open();

                final ZmqTextMessage inMessage2 = (ZmqTextMessage) receiver1.receive(3000);

                LOGGER.info("Elapse time (msec): " + stopwatch.elapsedTime());

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());
            } catch (ZmqException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                sender.close();
                receiver1.close();

                Thread.sleep(1000);

                context.close();
            }
        } catch (JMSException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }

        LOGGER.info("Finish Failover with Restart test.");
    }
}
