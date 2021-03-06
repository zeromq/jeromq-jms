package org.zeromq.jms.protocol;

import java.util.List;
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
    private static final String SOCKET_ADDR_1 = "tcp://*:9740";
    private static final String SOCKET_ADDR_2 = "tcp://*:9741";

    private static final String SOCKET_ADDR_3 = "tcp://*:9742";
    private static final String SOCKET_ADDR_4 = "tcp://*:9743";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";

    /**
     * Test a send and receive protocol functionality were a re-direct on fail-over.
     * @throws InterruptedException  throws interrupt exception on failed waits
     */
    @Test
    public void testFailoverWithRedirect() throws InterruptedException {
        LOGGER.info("Start Failover with Redirect test.");

        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext senderContext = new ZmqSocketContext(SOCKET_ADDR_1 + "," + SOCKET_ADDR_2, ZmqSocketType.DEALER, false, flags);
        final ZmqGateway sender = new ZmqParGateway("send12", senderContext,
                null, handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqSocketContext receiverContext1 = new ZmqSocketContext(SOCKET_ADDR_1, ZmqSocketType.ROUTER, true, flags);
        final ZmqGateway receiver1 = new ZmqParGateway("recv01", receiverContext1,
                  null, handler, null, null, null, null, false, Direction.INCOMING);

        final ZmqSocketContext receiverContext2 = new ZmqSocketContext(SOCKET_ADDR_2, ZmqSocketType.ROUTER, true, flags);
        final ZmqGateway receiver2 = new ZmqParGateway("recv02", receiverContext2,
                  null, handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();

            receiver1.open(-1);
            receiver2.open(-1);

            sender.open(-1);

            try {
                Stopwatch stopwatch = new Stopwatch();

                sender.send(outMessage1);

                // Ensure it has a change to get to the valid active receiver
                Thread.sleep(3000);

                ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver1.receive(3000);

                if (inMessage1 == null) {
                    LOGGER.info("Try receiver2 for message");
                    inMessage1 = (ZmqTextMessage) receiver2.receive(3000);
                    receiver2.close(-1);
                } else {
                    receiver1.close(-1);
                }

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                // Need to ensure switch-over
                Thread.sleep(5000);

                LOGGER.info("Elapse time (msec): " + stopwatch.lapsedTime());

                sender.send(outMessage2);

                ZmqTextMessage inMessage2 = null;

                if (receiver1.isActive()) {
                    inMessage2 = (ZmqTextMessage) receiver1.receive(3000);
                } else {
                    inMessage2 = (ZmqTextMessage) receiver2.receive(3000);
                }

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());
            } catch (ZmqException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                LOGGER.info("Closing sender gateway");

                sender.close(-1);

                LOGGER.info("Closing receiver gateway");

                if (receiver1.isActive()) {
                    receiver1.close(-1);
                }
                if (receiver2.isActive()) {
                    receiver2.close(-1);
                }

                LOGGER.info("Closing context");
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

        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext senderContext = new ZmqSocketContext(SOCKET_ADDR_3 + "," + SOCKET_ADDR_4, ZmqSocketType.DEALER, false, flags);
        final ZmqGateway sender = new ZmqParGateway("send12", senderContext,
                null, handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqSocketContext receiverContext1 = new ZmqSocketContext(SOCKET_ADDR_3, ZmqSocketType.ROUTER, true, flags);
        final ZmqGateway receiver1 = new ZmqParGateway("recv01", receiverContext1,
                  null, handler, null, null, null, null, false, Direction.INCOMING);

        final ZmqSocketContext receiverContext2 = new ZmqSocketContext(SOCKET_ADDR_4, ZmqSocketType.ROUTER, true, flags);
        final ZmqGateway receiver2 = new ZmqParGateway("recvr02", receiverContext2,
                  null, handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();

            LOGGER.info("Open sender & receivers.");

            sender.open(-1);

            receiver1.open(-1);
            receiver2.open(-1);

            try {
                Stopwatch stopwatch = new Stopwatch();

                LOGGER.info("Send message1: " + outMessage1);

                sender.send(outMessage1);

                Thread.sleep(3000);

                ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver1.receive(3000);

                if (inMessage1 == null) {
                    LOGGER.info("Try receiver2 for message");
                    inMessage1 = (ZmqTextMessage) receiver2.receive(3000);
                }

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                LOGGER.info("Received message1: " + inMessage1);
                LOGGER.info("Elapse time (msec): " + stopwatch.lapsedTime());

                dumpMetrics(sender);
                dumpMetrics(receiver1);
                dumpMetrics(receiver2);

                Thread.sleep(3000);

                LOGGER.info("Close receivers");

                receiver1.close(-1);
                receiver2.close(-1);

                Thread.sleep(3000);

                stopwatch = new Stopwatch();

                dumpMetrics(sender);
                dumpMetrics(receiver1);
                dumpMetrics(receiver2);

                LOGGER.info("Send message2: " + outMessage2);

                sender.send(outMessage2);

                Thread.sleep(3000);

                LOGGER.info("Open receivers1 again");

                receiver1.open(-1);

                Thread.sleep(3000);

                dumpMetrics(sender);
                dumpMetrics(receiver1);
                dumpMetrics(receiver2);

                final ZmqTextMessage inMessage2 = (ZmqTextMessage) receiver1.receive(3000);

                LOGGER.info("Elapse time (msec): " + stopwatch.lapsedTime());

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());

                LOGGER.info("Received message2: " + inMessage2);
            } catch (Throwable ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                sender.close(-1);
                receiver1.close(-1);
            }
        } catch (JMSException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }

        LOGGER.info("Finish Failover with Restart test.");
    }

    /**
     * Dump the metrics for the gateway.
     * @param gateway  the gateway
     */
    private void dumpMetrics(final ZmqGateway gateway) {
        final List<ZmqSocketMetrics> metricList = gateway.getMetrics();

        for (ZmqSocketMetrics metrics : metricList) {
            LOGGER.info("Dump [" + gateway.getName() + "] metrics: " + metrics.toString());
        }

    }
}
