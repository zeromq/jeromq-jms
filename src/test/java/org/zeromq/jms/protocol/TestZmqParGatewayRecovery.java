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
import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.jms.ZmqTextMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;
import org.zeromq.jms.protocol.ZmqGateway.Direction;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.event.ZmqStompEventHandler;

/**
 * Test for PAR gateway fail-over protocol functionality.
 *
 * NOTE:
 * This is a non test since ZMQ cannot re-use socket within a process, hence "already bound"
 * will always be the case once a process use it.
 */
public class TestZmqParGatewayRecovery {

    private static final Logger LOGGER = Logger.getLogger(TestZmqParGatewayRecovery.class.getCanonicalName());

    private static final String SOCKET_ADDR_1 = "tcp://*:9750";
    private static final String SOCKET_ADDR_2 = "tcp://*:9751";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    /**
     * Test a send and receive protocol functionality were a re-direct on fail-over.
     * @throws InterruptedException  throws interrupt exception on failed waits
     */
    @Test
    public void testRecoverySendMessages() throws InterruptedException {
        LOGGER.info("Start Failover with Redirect test.");

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext senderContext = new ZmqSocketContext(SOCKET_ADDR_1 + "," + SOCKET_ADDR_2, ZmqSocketType.DEALER, false, flags);
        final ZmqGateway sender = new ZmqParGateway("send1", context, senderContext,
                null, handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqSocketContext receiverContext = new ZmqSocketContext(SOCKET_ADDR_1 + "," + SOCKET_ADDR_2, ZmqSocketType.ROUTER, true, flags);
        final ZmqGateway receiver1 = new ZmqParGateway("recv1", context, receiverContext,
                  null, handler, null, null, null, null, false, Direction.INCOMING);
        final ZmqGateway receiver2 = new ZmqParGateway("recv2", context, receiverContext,
                 null, handler, null, null, null, null, false, Direction.INCOMING);
        final ZmqGateway receiver3 = new ZmqParGateway("recv3", context, receiverContext,
                null, handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();
            final ZmqTextMessage outMessage3 = ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage();

            try {
                receiver1.open();

                sender.open();
                sender.send(outMessage1);

                ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver1.receive(3000);

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                LOGGER.info("Open gateways 3");
                receiver2.open();
                // Ensure socket2 gets the binding
                Thread.sleep(3000);
                receiver3.open();

                LOGGER.info("Closing gateways 1");
                receiver1.close();

                LOGGER.info("Send message 2");
                sender.send(outMessage2);

                ZmqTextMessage inMessage2 = (ZmqTextMessage) receiver2.receive(3000);

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());

                LOGGER.info("Closing gateways 1 & 2");

                Thread.sleep(3000);

                LOGGER.info("Send message 3");

                sender.send(outMessage3);

                ZmqTextMessage inMessage3 = (ZmqTextMessage) receiver3.receive(3000);

                Assert.assertNull(inMessage3);

                receiver3.close();
            } catch (RuntimeException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                LOGGER.info("Closing gateways (sender & 3)");

                sender.close();

                receiver1.close();
                receiver2.close();
                receiver3.close();

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
     * Test a send and receive protocol functionality were a re-direct on fail-over.
     * @throws InterruptedException  throws interrupt exception on failed waits
     */
    @Test
    public void testRedundencyServer() throws InterruptedException {
        LOGGER.info("Start Failover with Redirect test.");

        final ZMQ.Context context1 = ZMQ.context(1);
        final ZMQ.Context context2 = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext receiverContext = new ZmqSocketContext(SOCKET_ADDR_1 + "," + SOCKET_ADDR_2, ZmqSocketType.ROUTER, true, flags);
        final ZmqGateway receiver1 = new ZmqParGateway("recv1", context1, receiverContext,
                  null, handler, null, null, null, null, false, Direction.INCOMING);
        final ZmqGateway receiver2 = new ZmqParGateway("recv2", context1, receiverContext,
                 null, handler, null, null, null, null, false, Direction.INCOMING);
        final ZmqGateway receiver3 = new ZmqParGateway("recv3", context2, receiverContext,
                null, handler, null, null, null, null, false, Direction.INCOMING);

        receiver1.open();
        receiver2.open();

        Thread.sleep(3000);

        receiver3.open();

        Thread.sleep(3000);

        receiver1.close();

        Thread.sleep(5000);

        receiver2.close();
        receiver3.close();

        context1.close();
        context2.close();

        LOGGER.info("Finish Recover with Redirect test.");
    }
}
