package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Zero MQ JMS Topic behaviour.
 */
public class TestZmqTopic {

    private static final String TOPIC_ADDR1 = "tcp://*:9720";
    private static final String TOPIC_ADDR2 = "tcp://*:9721";
    private static final String TOPIC_ADDR3 = "tcp://*:9722";

    private static final String TOPIC_URI1 = "jms:topic:topic_1?socket.addr=" + TOPIC_ADDR1 + "&event=stomp";
    private static final String TOPIC_URI2 = "jms:topic:topic_2?socket.addr=" + TOPIC_ADDR2 + "&event=stomp";
    private static final String TOPIC_URI3 = "jms:topic:topic_3?socket.addr=" + TOPIC_ADDR3 + "&event=stomp";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    /**
     * Test a send and receive JMS message functionality.
     */
    @Test
    public void testPublishedAndSubscribeMessageWithoutTransaction() {

        try {
            final TopicConnectionFactory factory = new ZmqConnectionFactory();
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(TOPIC_URI1);

            TopicPublisher publisher = null;
            TopicSubscriber subscriber1 = null;
            TopicSubscriber subscriber2 = null;

            try {
                publisher = session.createPublisher(topic);

                subscriber1 = session.createSubscriber(topic);
                subscriber2 = session.createSubscriber(topic);

                Thread.sleep(100);

                try {
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }

                try {
                    TextMessage message1 = (TextMessage) subscriber1.receive(1000);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_1, message1.getText());

                    Thread.sleep(1000);

                    TextMessage message2 = (TextMessage) subscriber1.receive();

                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_2, message2.getText());

                    TextMessage message3 = (TextMessage) subscriber1.receiveNoWait();

                    Assert.assertNotNull(message3);
                    Assert.assertEquals(MESSAGE_3, message3.getText());

                    message1 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_1, message1.getText());

                    message2 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_2, message2.getText());

                    message3 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message3);
                    Assert.assertEquals(MESSAGE_3, message3.getText());
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            } finally {
                session.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test a send and receive JMS message functionality.
     */
    @Test
    public void testPublishedAndSubscribeMessageWithTransaction() {

        try {
            final TopicConnectionFactory factory = new ZmqConnectionFactory();
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(TOPIC_URI2);

            TopicPublisher publisher = null;
            TopicSubscriber subscriber1 = null;
            TopicSubscriber subscriber2 = null;

            try {
                publisher = session.createPublisher(topic);

                Thread.sleep(100);

                subscriber1 = session.createSubscriber(topic);
                subscriber2 = session.createSubscriber(topic);

                Thread.sleep(100);

                try {
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

                    session.commit();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }

                Thread.sleep(100);

                try {
                    TextMessage message1 = (TextMessage) subscriber1.receive(1000);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_1, message1.getText());

                    TextMessage message2 = (TextMessage) subscriber1.receive();

                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_2, message2.getText());

                    TextMessage message3 = (TextMessage) subscriber1.receiveNoWait();

                    Assert.assertNotNull(message3);
                    Assert.assertEquals(MESSAGE_3, message3.getText());

                    message1 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_1, message1.getText());

                    message2 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_2, message2.getText());

                    message3 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message3);
                    Assert.assertEquals(MESSAGE_3, message3.getText());
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            } finally {
                session.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }

    }

    /**
     * Test a send and Listener JMS message functionality.
     */
    @Test
    public void testMessageListener() {
        try {
            final TopicConnectionFactory factory = new ZmqConnectionFactory();
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(TOPIC_URI3);

            TopicPublisher publisher = null;
            TopicSubscriber subscriber = null;

            try {
                publisher = session.createPublisher(topic);

                subscriber = session.createSubscriber(topic, "Region IN ('NASA','APAC')", false);

                final List<TextMessage> sendMessages = new ArrayList<TextMessage>();

                // User a count down latch to find 2 ofthe 3 messages since we have specified
                // "Region IN ('NASA','APAC')"
                final CountDownLatch messageCountDownLatch = new CountDownLatch(2);

                subscriber.setMessageListener(new MessageListener() {

                    @Override
                    public void onMessage(final Message message) {
                        final TextMessage textMessage = (TextMessage) message;

                        try {
                            final String sendMessage = textMessage.getText();
                            sendMessages.add(textMessage);
                            Assert.assertNotNull(sendMessage);

                        } catch (JMSException ex) {
                            Assert.fail(ex.getMessage());
                        }

                        messageCountDownLatch.countDown();
                    }
                });

                Thread.sleep(100);

                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).appendProperty("Region", "EMEA").toMessage());
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).appendProperty("Region", "APAC").toMessage());
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).appendProperty("Region", "NASA").toMessage());

                try {
                    messageCountDownLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                Assert.assertEquals(2, sendMessages.size());
            } finally {
                session.close();
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
