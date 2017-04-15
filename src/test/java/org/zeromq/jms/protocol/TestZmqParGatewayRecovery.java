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
                receiver1.open(-1);

                sender.open(-1);
                sender.send(outMessage1);

                ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver1.receive(5000);

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                LOGGER.info("Open gateways 3");
                receiver2.open(-1);
                // Ensure socket2 gets the binding
                Thread.sleep(3000);
                receiver3.open(-1);

                LOGGER.info("Closing gateways 1");
                receiver1.close(-1);

                LOGGER.info("Send message 2");
                sender.send(outMessage2);

                ZmqTextMessage inMessage2 = (ZmqTextMessage) receiver2.receive(5000);

                /*
                 * NOTE:
                 * There ia a 50% change that socket "tcp://*:9751" picked up the MESSAGE_1
                 * first, and although later returns the message for "tcp://*:9750" to send
                 * it, that actual ZMQ.Socket will still have the copy and still be trying.
                 *
                 * Hence with the open of other receiver, they will get this PHANTOM message
                 * before MESSAGE_@ comes through.
                 *
                 * This will not happen if the sender is closed, and then re-opened.
                 */
                Assert.assertNotNull(inMessage2);
                if (MESSAGE_1.equals(inMessage2.getText())) {
                    // Phantom message, read the next
                    inMessage2 = (ZmqTextMessage) receiver2.receive(5000);
                }

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());

                LOGGER.info("Closing gateways 1 & 2");

                Thread.sleep(3000);

                LOGGER.info("Send message 3");

                sender.send(outMessage3);

                ZmqTextMessage inMessage3 = (ZmqTextMessage) receiver3.receive(3000);

                Assert.assertNull(inMessage3);

                receiver3.close(-1);
            } catch (AssertionError ex) {
                throw ex;
            } catch (RuntimeException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                LOGGER.info("Closing gateways (sender & 3)");

                sender.close(-1);

                receiver1.close(-1);
                receiver2.close(-1);
                receiver3.close(-1);

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

        receiver1.open(-1);
        receiver2.open(-1);

        Thread.sleep(3000);

        receiver3.open(-1);

        Thread.sleep(3000);

        receiver1.close(-1);

        Thread.sleep(5000);

        receiver2.close(-1);
        receiver3.close(-1);

        context1.close();
        context2.close();

        LOGGER.info("Finish Recover with Redirect test.");
    }
}
