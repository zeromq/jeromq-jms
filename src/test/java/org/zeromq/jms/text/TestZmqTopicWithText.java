package org.zeromq.jms.text;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.jms.ZmqConnectionFactory;

/**
 *  Test the Zero MQ Test Topic subscriber from RAW JEROMQ publisher. The real world.
 */
public class TestZmqTopicWithText {

   private static final String TYPE_1 = "eventType";
   private static final String MESSAGE_1 = "this is the text message 1";

   /**
    * Test a send and receive JMS message functionality.
    * @throws JMSException  throws JMS exception
    */
   @Test
   public void testRawPubAndSubscribeMessage() throws JMSException {
       final String subscriberUri = "jms:topic:all?socket.addr=tcp://*:9714&event=text";
       final String[] destinations = new String[] { subscriberUri };
       
       try {
           Context context = ZMQ.context(1);

           final Socket publisherSocket = context.socket(ZMQ.PUB);

           publisherSocket.bind("tcp://*:9714");

           final Socket subscriberSocket = context.socket(ZMQ.SUB);

           subscriberSocket.connect("tcp://*:9714");
           subscriberSocket.subscribe(ZMQ.SUBSCRIPTION_ALL);

           final TopicConnectionFactory factory = new ZmqConnectionFactory(destinations);
           final TopicConnection connection = factory.createTopicConnection();
           final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

           final CachingConnectionFactory sharedFactory = new CachingConnectionFactory();

           try {
               final Topic topic = session.createTopic("all");

               TopicSubscriber subscriber = session.createSubscriber(topic);

               sharedFactory.setTargetConnectionFactory(factory);
               sharedFactory.setClientId("test");
               sharedFactory.setSessionCacheSize(1);

               final JmsTemplate template = new JmsTemplate();

               template.setPubSubDomain(true);
               template.setConnectionFactory(sharedFactory);
               template.setDefaultDestinationName(subscriberUri);
               template.setPubSubNoLocal(false);
               template.setReceiveTimeout(100);

               // Start the pool connection to ensure ZMQ gateway started.
               final Object templateNoMessage = template.receiveAndConvert();

               Assert.assertNull(templateNoMessage);

               // Write two messages, each with an envelope and content
               publisherSocket.sendMore("eventType");

               final boolean result = publisherSocket.send(MESSAGE_1);

               Assert.assertTrue(result);

               final String subscriberMessage1 = subscriberSocket.recvStr();
               final String subscriberMessage2 = subscriberSocket.recvStr();

               Assert.assertEquals(TYPE_1, subscriberMessage1);
               Assert.assertEquals(MESSAGE_1, subscriberMessage2);

               final TextMessage topicMessage = (TextMessage) subscriber.receive(1000);

               Assert.assertEquals(TYPE_1, topicMessage.getJMSType());
               Assert.assertEquals(MESSAGE_1, topicMessage.getText());

               final Object templateMessage = (Object) template.receiveAndConvert();

               Assert.assertEquals(MESSAGE_1, templateMessage);
           } finally {
               session.close();
               sharedFactory.destroy();
               subscriberSocket.close();
               publisherSocket.close();
           }
       } catch (Exception ex) {
           ex.printStackTrace();
           Assert.fail(ex.getMessage());
       }
   }
}
