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
public class TestZmqTopicWithMultiProducers {

    private static final String TOPIC_ADDR1 = "tcp://*:9735";
    private static final String TOPIC_ADDR2 = "tcp://*:9736";
 
    private static final String TOPIC_URI = "jms:topic:topic_2?socket.addr=" + TOPIC_ADDR1 + "," + TOPIC_ADDR2
        + "&filter=propertyTag&filter.subTags=NASA,APAC&filter.pubPropertyName=Region&event=stomp";

    private static final String TOPIC_ADDR3 = "tcp://*:9737";
    private static final String TOPIC_ADDR4 = "tcp://*:9738";

    private static final String TOPIC_PUB = "jms:topic:publish?socket.addr=" + TOPIC_ADDR3 + "&socket.bind=false"
            + "&filter=propertyTag&filter.pubPropertyName=Region&event=stomp";
    private static final String TOPIC_SUB1 = "jms:topic:subscribe1?proxy.proxyAddr=" + TOPIC_ADDR3 + "&proxy.proxyType=XSUB&proxy.proxyOutType=XPUB" 
            + "&socket.addr=" + TOPIC_ADDR4 + "&socket.bind=false"
            + "&filter=propertyTag&filter.subTags=NASA,APAC&event=stomp";
    private static final String TOPIC_SUB2 = "jms:topic:subscribe2?proxy.proxyAddr=" + TOPIC_ADDR3 + "&proxy.proxyType=XSUB&proxy.proxyOutType=XPUB" 
            + "&socket.addr=" + TOPIC_ADDR4 + "&socket.bind=false"
            + "&filter=propertyTag&filter.subTags=EMEA&event=stomp";

    private static final String MESSAGE_1 = "this is the text message EMEA";
    private static final String MESSAGE_2 = "this is the text message APAC";
    private static final String MESSAGE_3 = "this is the text message NASA";

    /**
     * Test a send and Listener JMS message functionality.
     */
    @Test
    public void testPublishedAndSubscribe() {
        try {
            final TopicConnectionFactory factory = new ZmqConnectionFactory();
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(TOPIC_URI);

            TopicPublisher publisher = null;
            TopicSubscriber subscriber = null;

            try {
                publisher = session.createPublisher(topic);
                subscriber = session.createSubscriber(topic);

                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).appendProperty("Region", "EMEA").toMessage());
                // Over two sockets, so ensure order is kept
                Thread.sleep(1000);
                publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).appendProperty("Region", "APAC").toMessage());
                Thread.sleep(1000);
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

    /**
     * Test a send and Listener JMS message functionality.
     */
    @Test
    public void testXSubXPubProxy() {
        try {
            final TopicConnectionFactory factory = new ZmqConnectionFactory();
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topicPub  = session.createTopic(TOPIC_PUB);
            final Topic topicSub1 = session.createTopic(TOPIC_SUB1);
            final Topic topicSub2 = session.createTopic(TOPIC_SUB2);

            TopicPublisher publisher1 = null;
            TopicPublisher publisher2 = null;

            TopicSubscriber subscriber1 = null;
            TopicSubscriber subscriber2 = null;
            TopicSubscriber subscriber3 = null;

            try {
                publisher1 = session.createPublisher(topicPub);
                publisher2 = session.createPublisher(topicPub);

                subscriber1 = session.createSubscriber(topicSub1);
                subscriber2 = session.createSubscriber(topicSub1);
                subscriber3 = session.createSubscriber(topicSub2);

                publisher1.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).appendProperty("Region", "EMEA").toMessage());
                publisher2.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).appendProperty("Region", "APAC").toMessage());
                publisher1.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).appendProperty("Region", "NASA").toMessage());

                try {
                    TextMessage message1 = (TextMessage) subscriber1.receive(1000);
                    TextMessage message2 = (TextMessage) subscriber1.receive();
                    TextMessage message3 = (TextMessage) subscriber1.receiveNoWait();

                    Assert.assertNotNull(message1);
                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_2, message1.getText());
                    Assert.assertEquals(MESSAGE_3, message2.getText());
                    Assert.assertNull(message3);

                    message1 = (TextMessage) subscriber2.receive(1000);
                    message2 = (TextMessage) subscriber2.receive();
                    message3 = (TextMessage) subscriber2.receiveNoWait();

                    Assert.assertNotNull(message1);
                    Assert.assertNotNull(message2);
                    Assert.assertEquals(MESSAGE_2, message1.getText());
                    Assert.assertEquals(MESSAGE_3, message2.getText());
                    Assert.assertNull(message3);

                    message1 = (TextMessage) subscriber3.receive(1000);
                    message3 = (TextMessage) subscriber3.receiveNoWait();

                    Assert.assertNotNull(message1);
                    Assert.assertEquals(MESSAGE_1, message1.getText());
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
