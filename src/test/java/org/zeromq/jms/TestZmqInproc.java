package org.zeromq.jms;

import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
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
 * Test "inproc" ZMQ sockets for Queues and Topics.
 */
public class TestZmqInproc {
    private static final String MESSAGE_1 = "this is the text message 1";

    /**
     * Test a send and receive JMS message functionality.
     * @throws Exception  throws exception on failure
     */
    @Test
    public void testInprocPushPull() throws Exception {
        final String[] destinations = new String[] {
            "jms:queue:sender?socket.addr=inproc://queue1&socket.bind=true",
            "jms:queue:consumer?socket.addr=inproc://queue1&socket.bind=false"
        };

        final QueueConnectionFactory factory = new ZmqConnectionFactory(destinations);
        final QueueConnection connection = factory.createQueueConnection();
        final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            final Queue senderQueue = session.createQueue("sender");
            final QueueSender sender = session.createSender(senderQueue);

            final Queue consumerQueue = session.createQueue("consumer");
            final MessageConsumer consumer = session.createConsumer(consumerQueue);

            sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());

            TextMessage message1 = (TextMessage) consumer.receive(1000);

            Assert.assertNotNull(message1);
        } finally {
            session.close();
        }
    }

    /**
     * Test "inproc" address publish/Subscribe  JMS message functionality.
     * @throws Exception  throws exception on failure
     */
    @Test
    public void testInprocPubSub() throws Exception {
        final String[] destinations = new String[] {
            "jms:topic:publish?socket.addr=inproc://topic1&event=stomp",
            "jms:topic:subscribe?socket.addr=inproc://topic1&event=stomp"
        };

        final TopicConnectionFactory factory = new ZmqConnectionFactory(destinations);
        final TopicConnection connection = factory.createTopicConnection();
        final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            final Topic publishTopic = session.createTopic("publish");
            final Topic subscribeTopic = session.createTopic("subscribe");

            final TopicPublisher publisher = session.createPublisher(publishTopic);

            final TopicSubscriber subscriber1 = session.createSubscriber(subscribeTopic);
            final TopicSubscriber subscriber2 = session.createSubscriber(subscribeTopic);

            publisher.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());

            TextMessage message1 = (TextMessage) subscriber1.receive(1000);
            TextMessage message2 = (TextMessage) subscriber2.receive(1000);

            Assert.assertNotNull(message1);
            Assert.assertNotNull(message2);
        } finally {
            session.close();
        }
    }
}
