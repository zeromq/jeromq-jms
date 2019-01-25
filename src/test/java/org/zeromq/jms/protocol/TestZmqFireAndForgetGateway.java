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

        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext senderContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.PUSH, false, flags);
        final ZmqGateway sender = new ZmqFireAndForgetGateway("protocol:sender", senderContext,
                null, handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqSocketContext receiverContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.PULL, true, flags);
        final ZmqGateway receiver = new ZmqFireAndForgetGateway("protocol:receiver", receiverContext,
                 null, handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();

            sender.open(-1);
            receiver.open(-1);

            try {
                sender.send(outMessage);

                final ZmqTextMessage inMessage = (ZmqTextMessage) receiver.receive(1000);

                Assert.assertEquals(MESSAGE_1, inMessage.getText());
            } catch (ZmqException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                sender.close(-1);
                receiver.close(-1);
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

        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext senderContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.PUSH, false, flags);
        final ZmqGateway sender = new ZmqFireAndForgetGateway("protocol:sender", senderContext,
                null, handler, null, null, null, null, true, Direction.OUTGOING);

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

        final ZmqSocketContext receiverContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.PULL, true, flags);
        final ZmqGateway receiver = new ZmqFireAndForgetGateway("protocol:receiver", receiverContext,
                null, handler, listener, null, null, null, true, Direction.INCOMING);

        sender.open(-1);
        receiver.open(-1);

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
            sender.close(-1);
            receiver.close(-1);
        }
    }

    /**
     * Test a send and asynchronous receive protocol functionality without transactions.
     */
    @Test
    public void testPublishedAndSubscribeMessageWithoutTransaction() {

        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();
        final ZmqFixedFilterPolicy filter = new ZmqFixedFilterPolicy();

        filter.setPublishTags("tag1");
        filter.setSubscribeTags(new String[] { "tag1", "tag2" });

        final ZmqSocketContext publisherContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.PUB, true, flags);
        final ZmqGateway publisher = new ZmqFireAndForgetGateway("protocol:publisher", publisherContext,
                 filter, handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqSocketContext subscriberContext1 = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.SUB, false, flags);
        final ZmqGateway subscriber1 = new ZmqFireAndForgetGateway("protocol:subscriber1", subscriberContext1,
                  filter, handler, null, null, null, null, false, Direction.INCOMING);

        final ZmqSocketContext subscriberContext2 = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.SUB, false, flags);
        final ZmqGateway subscriber2 = new ZmqFireAndForgetGateway("protocol:subscriber2", subscriberContext2,
                  filter, handler, null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();

            publisher.open(-1);
            subscriber1.open(-1);
            subscriber2.open(-1);

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
                publisher.close(-1);
                subscriber1.close(-1);
                subscriber2.close(-1);
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
        final int flags = 0;
        final ZmqFilterPolicy filter = new ZmqFixedFilterPolicy();
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqSocketContext publisherContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.PUB, true, flags);
        final ZmqGateway publisher = new ZmqFireAndForgetGateway("protocol:publisher", publisherContext,
                  filter, handler, null, null, null, null, false, Direction.OUTGOING);

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

            final ZmqSocketContext subscriberContext = new ZmqSocketContext(SOCKET_ADDR, ZmqSocketType.SUB, false, flags);
            final ZmqGateway subscriber = new ZmqFireAndForgetGateway("protocol:subscriber", subscriberContext,
                    filter, handler, listener, null, selector, null, false, Direction.INCOMING);

            publisher.open(-1);
            subscriber.open(-1);

            try {
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
                publisher.close(-1);
                subscriber.close(-1);
            }
        } catch (JMSException | ParseException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }
    }
}
