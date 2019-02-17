package org.zeromq.jms;

import javax.jms.JMSException;
import javax.jms.Message;
/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
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
public class TestZmqQueueWithProxy {

    private static final String SENDER_NAME = "sender";
    private static final String SENDER_ADDR = "tcp://*:9738";
    private static final String SENDER_URI = "jms:queue:" + SENDER_NAME + "?socket.addr=" + SENDER_ADDR + "&event=stomp";

    private static final String RECEIVER_NAME = "receiver";
    private static final String RECEIVER_ADDR = "tcp://*:9329";
    private static final String RECEIVER_URI =
        "jms:queue:" + RECEIVER_NAME + "?proxy.proxyAddr=" + SENDER_ADDR + "&proxy.proxyType=PULL&proxy.proxyOutType=PUSH&socket.addr=" + RECEIVER_ADDR + "&socket.bind=false&event=stomp";

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

        context.bind("java:/comp/env/jms/queueConnectionFactory", new ZmqConnectionFactory(new String[] { SENDER_URI, RECEIVER_URI }));
        context.bind("java:/comp/env/jms/" + SENDER_NAME, new ZmqQueue(SENDER_NAME));
        context.bind("java:/comp/env/jms/" + RECEIVER_NAME, new ZmqQueue(RECEIVER_NAME));
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
    public void test1Sender1RecieverWithProxy() {

        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queueSender = (Queue) context.lookup("java:/comp/env/jms/sender");
            final Queue queueReceiver = (Queue) context.lookup("java:/comp/env/jms/receiver");

            QueueSender sender = null;
            QueueReceiver receiver = null;

            try {
                sender = session.createSender(queueSender);
                receiver = session.createReceiver(queueReceiver);

                try {
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());
                } catch (Exception ex) {
                    throw ex;
                }

                try {
                    TextMessage message1 = (TextMessage) receiver.receive(1000);
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
     * Loop through all received to find a message to process.
     * @param  receivers      the array of receivers
     * @param  waitTime       the wait time (milliseconds)
     * @return                return null or the first found message
     * @throws JMSException   throws JMS exception on an error
     */
    private Message recieveMessage(final QueueReceiver[] receivers, final int waitTime) throws JMSException {
        for (QueueReceiver receiver : receivers) {
            Message message = receiver.receive(waitTime);

            if (message != null) {
                return message;
            }
        }

        return null;
    }
    /**
     * Test a send and receive JMS message functionality. This instance uses the ADDR as the name ad address so not
     * schema URI data is required.
     */
    @Test
    public void test2Sender2RecieverWithProxy() {

        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queueSender = (Queue) context.lookup("java:/comp/env/jms/sender");
            final Queue queueReceiver = (Queue) context.lookup("java:/comp/env/jms/receiver");

            QueueSender sender1 = null;
            QueueSender sender2 = null;
            QueueReceiver receiver1 = null;
            QueueReceiver receiver2 = null;

            try {
                sender1 = session.createSender(queueSender);
                sender2 = session.createSender(queueSender);
                receiver1 = session.createReceiver(queueReceiver);
                receiver2 = session.createReceiver(queueReceiver);

                final QueueReceiver[] receivers = new QueueReceiver[] { receiver1, receiver2 };

                try {
                    sender1.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    TextMessage message1 = (TextMessage) recieveMessage(receivers, 1000);

                    sender2.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    TextMessage message2 = (TextMessage) recieveMessage(receivers, 1000);

                    sender2.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());
                    TextMessage message3 = (TextMessage) recieveMessage(receivers, 1000);

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
}
