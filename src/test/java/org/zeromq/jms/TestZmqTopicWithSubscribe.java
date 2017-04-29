package org.zeromq.jms;

/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
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
public class TestZmqTopicWithSubscribe {

    private static final String TOPIC_ADDR1 = "tcp://*:9725";
    private static final String TOPIC_ADDR2 = "tcp://*:9726";

    private static final String TOPIC_URI1 = "jms:topic:topic_1?gateway.addr=" + TOPIC_ADDR1 + "&redelivery.retry=0&event=stomp";
    private static final String TOPIC_URI2 = "jms:topic:topic_2?gateway.addr=" + TOPIC_ADDR2 +
        "&filter=propertyTag&filter.subTags=NASA,APAC&filter.pubPropertyName=Region&event=stomp";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    /**
     * Test a send and receive JMS message functionality.
     */
    //@Test
    public void testSQLPublishedAndSubscribe() {

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

                subscriber1 = session.createSubscriber(topic, "Region IN ('NASA','APAC')", false);
                subscriber2 = session.createSubscriber(topic, "Region IN ('EMEA')", false);

                Thread.sleep(100);

                try {
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).appendProperty("Region", "EMEA").toMessage());
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).appendProperty("Region", "APAC").toMessage());
                    publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).appendProperty("Region", "NASA").toMessage());

                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }

                try {
                    TextMessage message1 = (TextMessage) subscriber1.receive(1000);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_2, message1.getText());

                    TextMessage message2 = (TextMessage) subscriber1.receive();

                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_3, message2.getText());

                    TextMessage message3 = (TextMessage) subscriber1.receiveNoWait();

                    Assert.assertNull(message3);

                    message1 = (TextMessage) subscriber2.receive(20);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_1, message1.getText());

                    message2 = (TextMessage) subscriber2.receiveNoWait();

                    Assert.assertNull(message2);
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
    public void testZmqPublishedAndSubscribe() {
        try {
            final TopicConnectionFactory factory = new ZmqConnectionFactory();
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(TOPIC_URI2);

            TopicPublisher publisher = null;
            TopicSubscriber subscriber = null;

            try {
                publisher = session.createPublisher(topic);
                subscriber = session.createSubscriber(topic);

                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).appendProperty("Region", "EMEA").toMessage());
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).appendProperty("Region", "APAC").toMessage());
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).appendProperty("Region", "NASA").toMessage());

                try {
                    TextMessage message1 = (TextMessage) subscriber.receive(1000);

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_2, message1.getText());

                    TextMessage message2 = (TextMessage) subscriber.receive();

                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_3, message2.getText());

                    TextMessage message3 = (TextMessage) subscriber.receiveNoWait();

                    Assert.assertNull(message3);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            } finally {
                session.close();
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
