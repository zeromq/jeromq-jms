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
 * Unit tests for Zero MQ JMS Queue behaviour.
 */
public class TestZmqQueue {

    private static final String QUEUE_NAME = "queue_1";
    private static final String QUEUE_ADDR = "tcp://*:9727";
    private static final String QUEUE_URI = "jms:queue:" + QUEUE_NAME + "?socket.addr=" + QUEUE_ADDR + "&event=stomp";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    private static InitialContext context;

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

        context.bind("java:/comp/env/jms/queueConnectionFactory", new ZmqConnectionFactory(new String[] { QUEUE_URI }));
        context.bind("java:/comp/env/jms/queueTest", new ZmqQueue(QUEUE_NAME));
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
     * Test a send and receive JMS message functionality. This instance uses the ADDR as the name ad address so not
     * schema URI data is required.
     */
    @Test
    public void testSendAndReceiveMessageWithoutTransaction() {

        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueTest");

            QueueSender sender = null;
            QueueReceiver receiver = null;

            try {
                sender = session.createSender(queue);
                receiver = session.createReceiver(queue);

                try {
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());
                } catch (Exception ex) {
                    throw ex;
                }

                try {
                    TextMessage message1 = (TextMessage) receiver.receive(1000);

                    Thread.sleep(1000);

                    TextMessage message2 = (TextMessage) receiver.receive();
                    TextMessage message3 = (TextMessage) receiver.receiveNoWait();

                    // sometime too quick to pickup since a transaction, so try again with a wait.
                    if (message3 == null) {
                        message3 = (TextMessage) receiver.receive(1000);
                    }

                    Assert.assertEquals(MESSAGE_1, message1.getText());
                    Assert.assertEquals(MESSAGE_2, message2.getText());
                    Assert.assertEquals(MESSAGE_3, message3.getText());
                } catch (Exception ex) {
                    throw ex;
                }
            } finally {
                session.close();
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test a send and receive JMS message functionality. This instance uses the ADDR as the name ad address so not
     * schema URI data is required.
     */
    @Test
    public void testSendAndReceiveMessageWithTransaction() {

        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueTest");

            QueueSender sender = null;
            QueueReceiver receiver = null;

            try {
                sender = session.createSender(queue);
                receiver = session.createReceiver(queue);

                try {
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

                    session.commit();
                } catch (Exception ex) {
                    throw ex;
                }

                try {
                    TextMessage message1 = (TextMessage) receiver.receive(2000);

                    Thread.sleep(1000);

                    TextMessage message2 = (TextMessage) receiver.receive();
                    TextMessage message3 = (TextMessage) receiver.receiveNoWait();

                    // sometime too quick to pickup since a transaction, so try again with a wait.
                    if (message3 == null) {
                        message3 = (TextMessage) receiver.receive(1000);
                    }

                    Assert.assertEquals(MESSAGE_1, message1.getText());
                    Assert.assertEquals(MESSAGE_2, message2.getText());
                    Assert.assertEquals(MESSAGE_3, message3.getText());
                } catch (Exception ex) {
                    throw ex;
                }
            } finally {
                session.close();
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test a send and Listener JMS message functionality.
     */
    @Test
    public void testMessageListenerWithoutTransaction() {
        try {
            final QueueConnectionFactory factory = new ZmqConnectionFactory(new String[] { QUEUE_URI });
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue(QUEUE_NAME);

            Assert.assertEquals(QUEUE_NAME, queue.getQueueName());

            QueueSender sender = null;
            QueueReceiver receiver = null;

            try {
                sender = session.createSender(queue);
                receiver = session.createReceiver(queue);
                final List<TextMessage> sendMessages = new ArrayList<TextMessage>();

                final CountDownLatch countDownLatch = new CountDownLatch(3);

                receiver.setMessageListener(new MessageListener() {

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

                        countDownLatch.countDown();
                    }
                });

                sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

                try {
                    countDownLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                Assert.assertEquals(3, sendMessages.size());
            } finally {
                session.close();
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test a send and Listener JMS message functionality.
     */
    @Test
    public void testMessageListenerWithTransaction() {
        try {
            final QueueConnectionFactory factory = new ZmqConnectionFactory(new String[] { QUEUE_URI });
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession senderSession = connection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
            final Queue senderQueue = senderSession.createQueue(QUEUE_NAME);

            Assert.assertEquals(QUEUE_NAME, senderQueue.getQueueName());

            final QueueSession receiverSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue receiverQueue = senderSession.createQueue(QUEUE_NAME);

            QueueSender sender = null;
            QueueReceiver receiver = null;

            try {
                sender = senderSession.createSender(senderQueue);
                receiver = receiverSession.createReceiver(receiverQueue);
                final List<TextMessage> sendMessages = new ArrayList<TextMessage>();

                final CountDownLatch countDownLatch = new CountDownLatch(3);

                receiver.setMessageListener(new MessageListener() {

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

                        countDownLatch.countDown();
                    }
                });

                sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

                senderSession.commit();

                try {
                    countDownLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                Assert.assertEquals(3, sendMessages.size());

            } finally {
                senderSession.close();
                receiverSession.close();
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
