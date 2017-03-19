package org.zeromq.jms;

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
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the Queue with Journal Store.
 */
public class TestZmqQueueWithJournalStore {

    private static final String QUEUE_NAME = "queue_1";
    private static final String QUEUE_ADDR = "tcp://*:9728";
    private static final String QUEUE_URI =
        "jms:queue:" + QUEUE_NAME
            + "?gateway=par&gateway.acknowldge=true&gateway.addr=" + QUEUE_ADDR
            + "&redlivery.retry=0&event=stomp" + "&journal=file";

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
    @Ignore("Jounraling still has issues") @Test
    public void testSendAndReceiveMessageWithoutTransaction() {

        try {
            final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory");
            final QueueConnection connection = factory.createQueueConnection();
            final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueTest");

            QueueSender sender = null;
            QueueReceiver receiver = null;

            try {
                // Create sender but no received so they will never be acknowledged
                sender = session.createSender(queue);

                try {
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
                    sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());
                } catch (Exception ex) {
                    throw ex;
                }

                sender.close();

                Thread.sleep(2000);

                // Restart sender, and start up receiver, so journal messages consumed
                sender = session.createSender(queue);

                Thread.sleep(2000);

                receiver = session.createReceiver(queue);

                Thread.sleep(2000);

                try {
                    TextMessage message1 = (TextMessage) receiver.receive();
                    TextMessage message2 = (TextMessage) receiver.receive();
                    TextMessage message3 = (TextMessage) receiver.receive();

                    Assert.assertEquals(MESSAGE_1, message1.getText());
                    Assert.assertEquals(MESSAGE_2, message2.getText());
                    Assert.assertEquals(MESSAGE_3, message3.getText());

                    Thread.sleep(2000);

                    TextMessage message4 = (TextMessage) receiver.receiveNoWait();

                    Assert.assertNull(message4);
                } catch (Exception ex) {
                    throw ex;
                }

                sender.close();
                receiver.close();

                // Check no journal records to be recieved again.
                receiver = session.createReceiver(queue);

                Thread.sleep(2000);

                try {
                    TextMessage message1 = (TextMessage) receiver.receiveNoWait();

                    Assert.assertNull(message1);
                } catch (Exception ex) {
                    throw ex;
                }

                receiver.close();
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
