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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Zero-JMS multi-clients.
 */
public class TestZmqQueueWithMultiSenders {

    private static final Logger LOGGER = Logger.getLogger(TestZmqQueueWithMultiSenders.class.getCanonicalName());

    private static final String QUEUE_ADDR = "tcp://*:9712";
    private static final String QUEUE_CLIENT_NAME = "send1";
    private static final String QUEUE_CLIENT_URI = "jms:queue:" + QUEUE_CLIENT_NAME
        + "?socket.addr=" + QUEUE_ADDR + "&socket.sndHWM=100000&redelivery.retry=0&event=stomp&ioThreads=3";
    private static final String QUEUE_SERVER_NAME = "recv1";
    private static final String QUEUE_SERVER_URI = "jms:queue:" + QUEUE_SERVER_NAME
        + "?socket.addr=" + QUEUE_ADDR + "&socket.bind=true&sndHWM=10000&redelivery.retry=0&event=stomp&ioThreads=3";

    private static final int CLIENT_COUNT = 10;

    // NOTE: Need to set high-water mark, otherwise flooding queue, and blocks, i.e. socket.setSndHwm(100000)
    private static final int CLIENT_MESSAGE_COUNT = 30000;
    private static final int CLIENT_MESSAGE_COMMIT_COUNT = 500;

    private static InitialContext context;

    /**
     * The test server class.
     */
    private static class Server implements MessageListener {

        private final CountDownLatch countDownLatch;
        private final AtomicInteger messageCount;

        /**
         * Construct test server.
         * @param countDownLatch  the server completed latch
         * @param messageCount    the message process count
         */
        private Server(final CountDownLatch countDownLatch, final AtomicInteger messageCount) {
            this.countDownLatch = countDownLatch;
            this.messageCount = messageCount;
        }

        @Override
        public void onMessage(final Message message) {
            final TextMessage textMessage = (TextMessage) message;
            messageCount.incrementAndGet();

            try {
                final String sendMessage = textMessage.getText();
                Assert.assertNotNull(sendMessage);

            } catch (JMSException ex) {
                LOGGER.log(Level.SEVERE, "Server failed to read message.", ex);
            }

            countDownLatch.countDown();
        }

    }

    /**
     * Test client runnable class.
     */

    private static class Client implements Runnable {
        private final String clientId;
        private final boolean transacted;
        private final CountDownLatch countDownLatch;

        /**
         * Construct the test client.
         * @param clientId        the unique client identifier
         * @param transacted      the transaction indicator
         * @param countDownLatch  the counter down latch
         */
        private Client(final String clientId, final boolean transacted, final CountDownLatch countDownLatch) {
            this.clientId = clientId;
            this.transacted = transacted;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Starting client: " + this);

                final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory_1");
                final QueueConnection connection = factory.createQueueConnection();
                final QueueSession session = connection.createQueueSession(transacted, Session.AUTO_ACKNOWLEDGE);
                final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueSend1");

                QueueSender sender = null;

                try {
                    sender = session.createSender(queue);

                    for (int i = 0; i < CLIENT_MESSAGE_COUNT; i++) {
                        sender.send(ZmqTextMessageBuilder.create().appendText("Message [" + clientId + ": " + i + "]").toMessage());

                        if (transacted && i % CLIENT_MESSAGE_COMMIT_COUNT == 0) {
                            session.commit();
                        }
                    }

                    if (transacted) {
                        session.commit();
                    }
                } finally {
                    countDownLatch.await(60, TimeUnit.SECONDS);

                    session.close();
                }
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Client could not send messages: " + this, ex);
            }

            LOGGER.info("Stopping client: " + this);
        }

        @Override
        public String toString() {
            return "Client [clientId=" + clientId + ", transacted=" + transacted + "]";
        }
    }

    /**
     * Set-up JNDI context for JMS Zero MQ unit tests.
     * @throws NamingException  throws exception
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

        context.bind("java:/comp/env/jms/queueConnectionFactory_1",
            new ZmqConnectionFactory(new String[] { QUEUE_CLIENT_URI, QUEUE_SERVER_URI }));
        context.bind("java:/comp/env/jms/queueSend1", new ZmqQueue(QUEUE_CLIENT_NAME));
        context.bind("java:/comp/env/jms/queueRecv1", new ZmqQueue(QUEUE_SERVER_NAME));
    }

    /**
     * Tear-down JNDI context for JMS Zero MQ unit tests.
     * @throws NamingException  throws exception
     */
    @AfterClass
    public static void pulldown() throws NamingException {

        context = new InitialContext();
        context.destroySubcontext("java:");
        context.close();
    }

    /**
     * Test a send and Listener JMS message functionality.
     *  mvn test -Dtest=TestZmqQueueWithMultiClients#testMultiClientWithoutTransact
     */
    @Test
    public void testMultiClientWithoutTransact() {
        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory_1");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueRecv1");

            QueueReceiver receiver = null;

            try {
                receiver = session.createReceiver(queue);

                final int totalMessageCount = CLIENT_COUNT * CLIENT_MESSAGE_COUNT;

                final CountDownLatch countDownLatch = new CountDownLatch(totalMessageCount);
                final AtomicInteger messageCount = new AtomicInteger(0);
                final Server server = new Server(countDownLatch, messageCount);

                receiver.setMessageListener(server);

                Thread.sleep(1000);

                final ExecutorService executor = Executors.newFixedThreadPool(CLIENT_COUNT);

                final List<Client> clients = new ArrayList<Client>();
                for (int i = 0; i < CLIENT_COUNT; i++) {
                    final Client client = new Client("CLIENT_" + i, false, countDownLatch);
                    executor.execute(client);
                    clients.add(client);
                }

                try {
                    countDownLatch.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                executor.shutdown();
                executor.awaitTermination(10000, TimeUnit.MILLISECONDS);

                for (Client client : clients) {
                    LOGGER.info("Client state: " + client);
                }

                Assert.assertEquals(totalMessageCount, messageCount.get());
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
    public void testMultiClientWithTransact() {
        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory_1");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueRecv1");

            QueueReceiver receiver = null;

            try {
                receiver = session.createReceiver(queue);

                final int totalMessageCount = CLIENT_COUNT * CLIENT_MESSAGE_COUNT;

                final CountDownLatch countDownLatch = new CountDownLatch(totalMessageCount);
                final AtomicInteger messageCount = new AtomicInteger(0);
                final Server server = new Server(countDownLatch, messageCount);

                receiver.setMessageListener(server);

                Thread.sleep(3000);

                final List<Client> clients = new ArrayList<Client>();
                for (int i = 0; i < CLIENT_COUNT; i++) {
                    final Client client = new Client("CLIENT_" + i, true, countDownLatch);
                    new Thread(client).start();
                    clients.add(client);
                }

                try {
                    countDownLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                Thread.sleep(3000);

                for (Client client : clients) {
                    LOGGER.info("Client state: " + client);
                }

                Assert.assertEquals(totalMessageCount, messageCount.get());
            } finally {
                session.close();
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
