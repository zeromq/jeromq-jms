package org.zeromq.jms;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the JMS 2.0 API work correctly.
 */
public class TestSimplifiedZmqQueue {

    private static final String QUEUE_NAME = "queue_1";
    private static final String QUEUE_ADDR = "tcp://*:9710";
    private static final String QUEUE_URI = "jms:queue:" + QUEUE_NAME + "?gateway.addr=" + QUEUE_ADDR + "&redlivery.retry=0&event=stomp";

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
            final ConnectionFactory factory = (ConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory");

            try {
                final JMSContext jmsContext = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
                final Destination queue = (Destination) context.lookup("java:/comp/env/jms/queueTest");

                final JMSConsumer consumer = jmsContext.createConsumer(queue);
                final JMSProducer producer = jmsContext.createProducer();

                producer.send(queue, MESSAGE_1);
                producer.send(queue, MESSAGE_2);
                producer.send(queue, MESSAGE_3);

                TextMessage message1 = (TextMessage) consumer.receive(1000);

                Thread.sleep(1000);

                TextMessage message2 = (TextMessage) consumer.receive();
                TextMessage message3 = (TextMessage) consumer.receiveNoWait();

                // sometime too quick to pickup since a transaction, so try again with a wait.
                if (message3 == null) {
                    message3 = (TextMessage) consumer.receive(1000);
                }

                Assert.assertEquals(MESSAGE_1, message1.getText());
                Assert.assertEquals(MESSAGE_2, message2.getText());
                Assert.assertEquals(MESSAGE_3, message3.getText());
            } finally {
                context.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
}
