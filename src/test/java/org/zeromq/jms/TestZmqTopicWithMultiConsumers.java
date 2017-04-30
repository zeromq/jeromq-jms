package org.zeromq.jms;

import java.util.ArrayList;
import java.util.List;
/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Zero-JMS multi-clients TOPIC.
 */
public class TestZmqTopicWithMultiConsumers {

    private static final Logger LOGGER = Logger.getLogger(TestZmqTopicWithMultiConsumers.class.getCanonicalName());

    private static final String TOPIC_ADDR = "tcp://*:9713";
    private static final String TOPIC_PUB_NAME = "topicPub";
    private static final String TOPIC_PUB_URI = "jms:topic:" + TOPIC_PUB_NAME + "?socket.addr=" + TOPIC_ADDR + "&redelivery.retry=0&event=stomp";

    private static final String TOPIC_REC_NAME = "topicRec";
    private static final String TOPIC_REC_URI = "jms:topic:" + TOPIC_REC_NAME + "?socket.addr=" + TOPIC_ADDR + "&redelivery.retry=0&event=stomp";

    private static final int CLIENT_COUNT = 5;
    private static final int CLIENT_MESSAGE_COUNT = 100;
    private static final int CLIENT_MESSAGE_COMMIT_COUNT = 7000;

    private static InitialContext context;

    /**
     *  Class to present a client.
     */
    private static class Client extends Thread implements MessageListener {

        private final CountDownLatch clientStartedLatch;
        private final CountDownLatch clientStoppedLatch;
        private final CountDownLatch messageCountLatch;
        private final AtomicInteger messageCount;
        private final String clientId;
        private final boolean transacted;

        /**
         * Construct client.
         * @param clientStartedLatch  the started clients count-down latch
         * @param clientStoppedLatch  the finished clients count-down latch
         * @param messageCount        the total message to process
         * @param clientId            the unique client identifier
         * @param transacted          the transaction indicator
         */
        private Client(final CountDownLatch clientStartedLatch, final CountDownLatch clientStoppedLatch, final AtomicInteger messageCount,
                final String clientId, final boolean transacted) {

            this.clientStartedLatch = clientStartedLatch;
            this.clientStoppedLatch = clientStoppedLatch;
            this.messageCountLatch = new CountDownLatch(CLIENT_MESSAGE_COUNT);
            this.messageCount = messageCount;
            this.clientId = clientId;
            this.transacted = transacted;
        }

        /**
         * Thread run - process messages.
         */
        @Override
        public void run() {
            try {
                LOGGER.info("Starting client: " + clientId);

                final TopicConnectionFactory factory = (TopicConnectionFactory) context.lookup("java:/comp/env/jms/topicConnectionFactory");
                final TopicConnection connection = factory.createTopicConnection();
                final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                final Topic topic = (Topic) context.lookup("java:/comp/env/jms/topicRec");

                TopicSubscriber subscriber = null;

                try {
                    subscriber = session.createSubscriber(topic, null, transacted);

                    subscriber.setMessageListener(this);

                    // client now ready reading messages
                    clientStartedLatch.countDown();
                    LOGGER.info("Client Ready & Listening: " + clientId);

                    try {
                        messageCountLatch.await(100, TimeUnit.SECONDS);
                        Assert.assertEquals(clientId + " missing messages", 0, messageCountLatch.getCount());
                    } catch (InterruptedException ex) {
                        throw ex;
                    }
                } finally {
                    session.close();
                }
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Client " + clientId + " could not recieve messages.", ex);
            }

            // client now finished reading messages
            clientStoppedLatch.countDown();
            LOGGER.info("Stopping client: " + clientId + ", clientStoppedLatch=" + clientStoppedLatch.getCount());
        }

        @Override
        public void onMessage(final Message message) {
            final TextMessage textMessage = (TextMessage) message;

            try {
                final String sendMessage = textMessage.getText();
                Assert.assertNotNull(sendMessage);
                messageCount.getAndIncrement();
            } catch (JMSException ex) {
                Assert.fail(ex.getMessage());
            }

            messageCountLatch.countDown();
        }

        @Override
        public String toString() {
            return "Client [clientStartedLatch=" + clientStartedLatch + ", clientStoppedLatch=" + clientStoppedLatch
                    + ", messageCountLatch=" + messageCountLatch + ", messageCount=" + messageCount + ", clientId="
                    + clientId + ", transacted=" + transacted + "]";
        }
    }

    /**
     * Setup the JNI for testing.
     * @throws NamingException  throws JNI naming exception
     */
    @BeforeClass
    public static void setup() throws NamingException {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        context = new InitialContext();

        context.createSubcontext("java:");
        context.createSubcontext("java:/comp");
        context.createSubcontext("java:/comp/env");
        context.createSubcontext("java:/comp/env/jms");

        context.bind("java:/comp/env/jms/topicConnectionFactory",
            new ZmqConnectionFactory(new String[] { TOPIC_PUB_URI, TOPIC_REC_URI }));
        context.bind("java:/comp/env/jms/topicPub", new ZmqTopic(TOPIC_PUB_NAME));
        context.bind("java:/comp/env/jms/topicRec", new ZmqTopic(TOPIC_REC_NAME));
    }

    /**
     * Tear-down the JNI for testing.
     * @throws NamingException  throws JNI naming exception
     */
    @AfterClass
    public static void pulldown() throws NamingException {

        context = new InitialContext();
        context.destroySubcontext("java:");
        context.close();
    }

    /**
     * Test a send and Listener JMS message functionality.
     */
    @Test
    public void testMultiClient() {
        try {
            final boolean serverTransacted = false;
            final boolean clientTransacted = false;

            final TopicConnectionFactory factory = (TopicConnectionFactory) context.lookup("java:/comp/env/jms/topicConnectionFactory");
            final TopicConnection connection = factory.createTopicConnection();
            final TopicSession session = connection.createTopicSession(serverTransacted, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = (Topic) context.lookup("java:/comp/env/jms/topicPub");

            TopicPublisher publisher = null;

            try {
                publisher = session.createPublisher(topic);

                final int totalMessageCount = CLIENT_COUNT * CLIENT_MESSAGE_COUNT;

                final CountDownLatch clientStartedDownLatch = new CountDownLatch(CLIENT_COUNT);
                final CountDownLatch clientStoppedDownLatch = new CountDownLatch(CLIENT_COUNT);

                final AtomicInteger messageCount = new AtomicInteger(0);

                LOGGER.info("<<<< START CLIENTS >>>>");

                final List<Client> clients = new ArrayList<Client>();
                for (int i = 0; i < CLIENT_COUNT; i++) {
                    final Client client = new Client(clientStartedDownLatch, clientStoppedDownLatch, messageCount, "CLIENT_" + i, clientTransacted);
                    client.start();
                    clients.add(client);
                }

                // Wait till all the clients have started
                try {
                    clientStartedDownLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                Assert.assertEquals(0, clientStartedDownLatch.getCount());

                LOGGER.info("<<<< SENDING MESSAGES >>>>");

                Thread.sleep(3000);

                // Can send messages now
                for (int i = 0; i < CLIENT_MESSAGE_COUNT; i++) {
                    publisher.send(ZmqTextMessageBuilder.create().appendText("SERVER " + i).appendProperty("Region", "EMEA").toMessage());

                    if (serverTransacted) {
                        if (i % CLIENT_MESSAGE_COMMIT_COUNT == 0) {
                            session.commit();
                        }
                    }
                }

                if (serverTransacted) {
                    session.commit();
                }

                // Wait till all the clients have stopped
                try {
                    clientStoppedDownLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                LOGGER.info("<<<< COMPLETE CHECKS >>>>");

                for (Client client : clients) {
                    LOGGER.info("Client state: " + client);
                }

                Assert.assertEquals(0, clientStoppedDownLatch.getCount());
                Assert.assertEquals(totalMessageCount, messageCount.intValue());
            } finally {
                session.close();
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
