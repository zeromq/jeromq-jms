package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.text.ParseException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.ZmqTextMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;
import org.zeromq.jms.protocol.ZmqGateway.Direction;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.event.ZmqStompEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.protocol.filter.ZmqFixedFilterPolicy;
import org.zeromq.jms.selector.ZmqMessageSelector;
import org.zeromq.jms.selector.ZmqSimpleMessageSelector;

/**
 * Test Fire and Forget send/receive protocol functionality.
 */
public class TestZmqFireAndForgetGateway {

    private static final String SOCKET_ADDR = "tcp://*:9732";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    /**
     * Test a send and receive protocol functionality without transactions enabled.
     */
    @Test
    public void testSendAndReceiveMessageWithoutTransaction() {

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqGateway sender = new ZmqFireAndForgetGateway("protocol:sender", context, ZmqSocketType.PUSH, false, SOCKET_ADDR, flags, null,
                handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqGateway receiver = new ZmqFireAndForgetGateway("protocol:receiver", context, ZmqSocketType.PULL, true, SOCKET_ADDR, flags, null,
                handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();

            sender.open();
            receiver.open();

            try {
                sender.send(outMessage);

                final ZmqTextMessage inMessage = (ZmqTextMessage) receiver.receive(1000);

                Assert.assertEquals(MESSAGE_1, inMessage.getText());
            } catch (ZmqException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                sender.close();
                receiver.close();

                context.close();
            }
        } catch (JMSException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test a send and receive protocol functionality with transactions.
     */
    @Test
    public void testSendAndReceiveMessageWithTransaction() {

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqGateway sender = new ZmqFireAndForgetGateway("protocol:sender", context, ZmqSocketType.PUSH, false, SOCKET_ADDR, flags, null,
                handler, null, null, null, null, true, Direction.OUTGOING);

        final CountDownLatch messageCountDownLatch = new CountDownLatch(3);

        final ZmqGatewayListener listener = new ZmqGatewayListener() {

            @Override
            public void onMessage(final ZmqMessage message) {
                messageCountDownLatch.countDown();
            }

            @Override
            public void onException(final ZmqException ex) {
                ex.printStackTrace();
            }
        };

        final ZmqGateway receiver = new ZmqFireAndForgetGateway("protocol:receiver", context, ZmqSocketType.PULL, true, SOCKET_ADDR, flags, null,
                handler, listener, null, null, null, true, Direction.INCOMING);

        sender.open();
        receiver.open();

        try {
            sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
            sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
            sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

            sender.commit();
            try {
                messageCountDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                throw ex;
            }

            Assert.assertEquals(0, messageCountDownLatch.getCount());

        } catch (JMSException | InterruptedException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        } finally {
            sender.close();
            receiver.close();

            context.close();
        }
    }

    /**
     * Test a send and asynchronous receive protocol functionality without transactions.
     */
    @Test
    public void testPublishedAndSubscribeMessageWithoutTransaction() {

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();
        final ZmqFilterPolicy filter = new ZmqFixedFilterPolicy();

        filter.setFilters(new String[] { ZmqFilterPolicy.DEFAULT_FILTER });

        final ZmqGateway publisher = new ZmqFireAndForgetGateway("protocol:publisher", context, ZmqSocketType.PUB, true, SOCKET_ADDR, flags, filter,
                handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqGateway subscriber1 = new ZmqFireAndForgetGateway("protocol:subscriber1", context, ZmqSocketType.SUB, false, SOCKET_ADDR, flags,
                filter, handler, null, null, null, null, false, Direction.INCOMING);

        final ZmqGateway subscriber2 = new ZmqFireAndForgetGateway("protocol:subscriber2", context, ZmqSocketType.SUB, false, SOCKET_ADDR, flags,
                filter, handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();

            publisher.open();
            subscriber1.open();
            subscriber2.open();

            try {
                Thread.sleep(100);

                publisher.send(outMessage);

                final ZmqTextMessage inMessage1 = (ZmqTextMessage) subscriber1.receive(1000);

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                final ZmqTextMessage inMessage2 = (ZmqTextMessage) subscriber2.receive(1000);

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_1, inMessage2.getText());
            } catch (ZmqException | InterruptedException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                publisher.close();
                subscriber1.close();
                subscriber2.close();

                context.close();
            }
        } catch (JMSException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test a send and asynchronous receive protocol functionality with transactions.
     */
    @Test
    public void testPublishedAndSubscribeMessageWithSelector() {
        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqFilterPolicy filter = new ZmqFixedFilterPolicy();
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqGateway publisher = new ZmqFireAndForgetGateway("protocol:publisher", context, ZmqSocketType.PUB, true, SOCKET_ADDR, flags, filter,
                handler, null, null, null, null, false, Direction.OUTGOING);

        // User a count down latch to find 2 of the 3 messages since we have specified "Region IN ('NASA','APAC')"
        final CountDownLatch messageCountDownLatch = new CountDownLatch(2);

        final ZmqGatewayListener listener = new ZmqGatewayListener() {

            @Override
            public void onMessage(final ZmqMessage message) {
                messageCountDownLatch.countDown();
            }

            @Override
            public void onException(final ZmqException ex) {
                ex.printStackTrace();
            }
        };

        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("Region IN ('NASA','APAC')");

            final ZmqGateway subscriber = new ZmqFireAndForgetGateway("protocol:subscriber", context, ZmqSocketType.SUB, false, SOCKET_ADDR, flags,
                    filter, handler, listener, null, selector, null, false, Direction.INCOMING);

            publisher.open();
            subscriber.open();

            try {
                Thread.sleep(100);

                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).appendProperty("Region", "EMEA").toMessage());
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).appendProperty("Region", "APAC").toMessage());
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).appendProperty("Region", "NASA").toMessage());

                try {
                    messageCountDownLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                Assert.assertEquals(0, messageCountDownLatch.getCount());

            } catch (ZmqException | InterruptedException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                publisher.close();
                subscriber.close();

                context.close();
            }
        } catch (JMSException | ParseException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }
    }
}
