package org.zeromq.jms;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.junit.Test;

import org.zeromq.jms.ZmqConnectionFactory;

/**
 *  Test the Zero MQ Test Topic subscriber from RAW JEROMQ publisher. The real world.
 */
public class TestZmqQueueWithExtendedUri {

   private static final String MESSAGE_1 = "this is the text message 1";

   /**
    * Test a send and receive JMS message functionality.
    * @throws Exception  throws exception on failure
    */
   @Test
   public void testPushPullWithExtends() throws Exception {
       final String[] destinations = new String[] {
           "jms:queue:base?socket.addr=tcp://*:9715",
           "jms:queue:consumer?extends=base&socket.bind=false"
       };
       
       final QueueConnectionFactory factory = new ZmqConnectionFactory(destinations);
       final QueueConnection connection = factory.createQueueConnection();
       final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

       try {
           final Queue consumerQueue = session.createQueue("consumer");
           final MessageConsumer consumer = session.createConsumer(consumerQueue);

           final Queue senderQueue = session.createQueue("jms:queue:sender?extends=base&socket.bind=true");
           final QueueSender sender = session.createSender(senderQueue);

           sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());

           TextMessage message1 = (TextMessage) consumer.receive(1000);

           Assert.assertNotNull(message1);
       } finally {
           session.close();
       }
   }
}
