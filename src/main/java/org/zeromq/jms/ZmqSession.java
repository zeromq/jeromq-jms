package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.management.ObjectName;

import org.zeromq.jms.jmx.ZmqMBeanUtils;
import org.zeromq.jms.protocol.ZmqGateway;
import org.zeromq.jms.protocol.ZmqGatewayFactory;
import org.zeromq.jms.protocol.ZmqSocketType;

/**
 * Generic Zero MQ JMS session for (Topic and Queue Consumers).
 */
public class ZmqSession implements QueueSession, TopicSession {

    private static final Logger LOGGER = Logger.getLogger(ZmqSession.class.getCanonicalName());

    private final Map<String, ZmqURI> destinationSchema;
    private final boolean transacted;
    private final int acknowledgeMode;
    private final ExceptionListener exceptionHandler;
    private final List<ZmqGateway> gateways = new ArrayList<ZmqGateway>();

    private static AtomicInteger gatewayProducerCount = new AtomicInteger(0);
    private static AtomicInteger gatewayConsumerCount = new AtomicInteger(0);

    private final ZmqGatewayFactory gatewayFactory;

    private final List<ObjectName> mbeanNames = new ArrayList<ObjectName>();

    /**
     * Package level queue session constructor.
     * @param gatewayFactory     the gateway factory
     * @param destinationSchema  the destination schema
     * @param transacted         session transacted indicates
     * @param acknowledgeMode    indicates whether the consumer or the client will acknowledge any messages it receives; ignored if the
     *                           session is transacted.
     * @param exceptionHandler   the exception handler for JMS exception.
     */
    ZmqSession(final ZmqGatewayFactory gatewayFactory, final Map<String, ZmqURI> destinationSchema, final boolean transacted,
            final int acknowledgeMode, final ExceptionListener exceptionHandler) {

        this.destinationSchema = destinationSchema;
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        this.exceptionHandler = exceptionHandler;
        this.gatewayFactory = gatewayFactory;
    }

    /**
     * Bind to a Zero MQ socket(s) to the address under specified within the gateway.
     * @param  gateway       the gateway containing the Zero MQ socket(s) to be bind
     * @throws JMSException  throws JMX exception
     */
    protected void open(final ZmqGateway gateway) throws JMSException {

        try {
            gateway.open(-1);

            List<ObjectName> objectNames = ZmqMBeanUtils.register(gateway);

            synchronized (mbeanNames) {
                mbeanNames.addAll(objectNames);
            }

            synchronized (gateways) {
                gateways.add(gateway);
            }
        } catch (RuntimeException ex) {
            LOGGER.log(Level.SEVERE, "Unable to open to ZMQ gateway: " + gateway, ex);

            throw new ZmqException("Unable to open to ZMQ gateway: " + gateway, ex);
        }
    }

    @Override
    public void close() throws JMSException {
        synchronized (gateways) {
            for (ZmqGateway gateway : gateways) {
                if (gateway.isActive()) {
                    gateway.close(-1);
                }
            }

            gateways.clear();
        }

        for (ObjectName objectName : mbeanNames) {
            ZmqMBeanUtils.unregister(objectName);
        }

        mbeanNames.clear();

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Session closed: " + this, new Exception("Who is calling close?"));
        } else {
            LOGGER.info("Session closed: " + this);
        }
    }

    @Override
    public void commit() throws JMSException {
        if (transacted) {
            try {
                synchronized (gateways) {
                    for (ZmqGateway gateway : gateways) {
                        gateway.commit();
                    }
                }
            } catch (ZmqException ex) {
                throw new ZmqException("Unable to commit messages: " + this, ex);
            }

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("Commited messages:" + this);
            }
        } else {
            throw new ZmqException("Session was not enabled for transactions.");
        }

        LOGGER.info("Session committed: " + this);
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MessageProducer createProducer(final Destination destination) throws JMSException {

        if (destination instanceof Topic) {
            final TopicPublisher producer = createPublisher((Topic) destination);

            return producer;
        }

        final QueueSender producer = createSender((Queue) destination);

        return producer;
    }

    @Override
    public MessageConsumer createConsumer(final Destination destination) throws JMSException {

        MessageConsumer consumer = createConsumer(destination, null, false);

        return consumer;
    }

    @Override
    public MessageConsumer createConsumer(final Destination destination, final String messageSelector) throws JMSException {

        final MessageConsumer consumer = createConsumer(destination, messageSelector, false);

        return consumer;
    }

    @Override
    public MessageConsumer createConsumer(final Destination destination, final String messageSelector, final boolean noLocal) throws JMSException {

        if (destination instanceof Topic) {
            final TopicSubscriber consumer = createSubscriber((Topic) destination, messageSelector, noLocal);

            return consumer;
        }

        final QueueReceiver consumer = createReceiver((Queue) destination, messageSelector);

        return consumer;
    }

    @Override
    public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public TopicSubscriber createDurableSubscriber(final Topic topic, final String name, final String messageSelector, final boolean noLocal)
            throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Message createMessage() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectMessage createObjectMessage(final Serializable object) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {

        final TextMessage message = new ZmqTextMessage();

        return message;
    }

    @Override
    public TextMessage createTextMessage(final String text) throws JMSException {

        final TextMessage message = createTextMessage();

        message.setText(text);

        return message;
    }

    @Override
    public Topic createTopic(final String topicName) throws JMSException {

        final ZmqURI uri = getOrPutUri(topicName);
        final Topic topic = new ZmqTopic(uri);

        return topic;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {

        return acknowledgeMode;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getTransacted() throws JMSException {

        return transacted;
    }

    @Override
    public void recover() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws JMSException {
        if (transacted) {
            try {
                synchronized (gateways) {
                    for (ZmqGateway gateway : gateways) {
                        gateway.rollback();
                    }
                }
            } catch (ZmqException ex) {
                throw new ZmqException("Unable to rollback messages", ex);
            }

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("rollback messages");
            }
        } else {
            throw new ZmqException("Session was not enabled for transactions.");
        }

        LOGGER.info("Session rolled back");
    }

    @Override
    public void run() {

        throw new UnsupportedOperationException();
    }

    @Override
    public void setMessageListener(final MessageListener listener) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe(final String name) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue, final String messageSelector) throws JMSException {

        throw new UnsupportedOperationException();
    }

    /**
     * Return the standard URIl, or the extended URI using a lookup of the schemas, or based on the value
     * passed in.
     * @param name           the URI name, or an actual URI starting with 'jms:....'.
     * @return               the basic URI or the extended URI
     * @throws ZmqException  throws the ZMQ exception when name or schema invalid
     */
    private ZmqURI getOrPutUri(final String name) throws ZmqException {
        String destinationName = name;

        if (name.startsWith("jms:queue") || name.startsWith("jms:topic")) {
            final ZmqURI uri = ZmqURI.create(name);
            destinationName = uri.getDestinationName();

            if (destinationSchema.containsKey(destinationName)) {
                LOGGER.warning("Creating queue with URI already exists in scheam: " + uri);
            } else {
                destinationSchema.put(destinationName, uri);
            }
        }

        if (!destinationSchema.containsKey(destinationName)) {
            throw new ZmqException("Unable to resolve queue within schema store for name: " + destinationName);
        }

        ZmqURI uri = destinationSchema.get(destinationName);

        if (ZmqExtendedURI.isExtened(uri)) {
            uri = new ZmqExtendedURI(uri, destinationSchema);
        }

        String addr = uri.getOptionValue("socket.addr", null);
        if (addr == null) {
            addr = uri.getOptionValue("gateway.addr", null);
        }

        if (addr == null) {
            throw new ZmqException("Unable to resolve 'socket.addr' or 'gateway.addr' for queue URI: " + uri);
        }

        return uri;
    }

    @Override
    public Queue createQueue(final String queueName) throws JMSException {

    	final ZmqURI uri = getOrPutUri(queueName);
        final Queue queue = new ZmqQueue(uri);

        return queue;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueueReceiver createReceiver(final Queue queue) throws JMSException {

        return createReceiver(queue, null);
    }

    @Override
    public QueueReceiver createReceiver(final Queue queue, final String messageSelector) throws JMSException {

        final ZmqQueue zmqQueue = (ZmqQueue) queue;
        final boolean transacted = getTransacted();
        final String prefixName = "receiver-" + gatewayConsumerCount.incrementAndGet();
        final ZmqGateway gateway = gatewayFactory.newConsumerGateway(prefixName, zmqQueue, ZmqSocketType.PULL, true, messageSelector,
                transacted);

        open(gateway);

        final QueueReceiver reciever = new ZmqQueueReciever(gateway, queue, messageSelector, exceptionHandler);

        LOGGER.info("Created recevier: " + reciever);

        return reciever;
    }

    @Override
    public QueueSender createSender(final Queue queue) throws JMSException {

        final ZmqQueue zmqQueue = (ZmqQueue) queue;
        final boolean transacted = getTransacted();
        final String prefixName = "sender-" + gatewayProducerCount.incrementAndGet();
        final ZmqGateway gateway = gatewayFactory.newProducerGateway(prefixName, zmqQueue, ZmqSocketType.PUSH, false, transacted);

        open(gateway);

        QueueSender sender = new ZmqQueueSender(gateway, queue);

        LOGGER.info("Created sender: " + sender);

        return sender;
    }

    @Override
    public TopicPublisher createPublisher(final Topic topic) throws JMSException {

        final ZmqTopic zmqTopic = (ZmqTopic) topic;
        final boolean transacted = getTransacted();
        final String prefixName = "publisher-" + gatewayProducerCount.incrementAndGet();
        final ZmqGateway gateway = gatewayFactory.newProducerGateway(prefixName, zmqTopic, ZmqSocketType.PUB, true, transacted);

        open(gateway);

        TopicPublisher publisher = new ZmqTopicPublisher(gateway, topic);

        LOGGER.info("Created publisher: " + publisher);

        return publisher;
    }

    @Override
    public TopicSubscriber createSubscriber(final Topic topic) throws JMSException {

        final TopicSubscriber subscriber = createSubscriber(topic, null, false);

        return subscriber;
    }

    @Override
    public TopicSubscriber createSubscriber(final Topic topic, final String messageSelector, final boolean noLocal) throws JMSException {

        final ZmqTopic zmqTopic = (ZmqTopic) topic;
        final boolean transacted = getTransacted();
        final String prefixName = "subscriber-" + gatewayConsumerCount.incrementAndGet();
        final ZmqGateway gateway = gatewayFactory.newConsumerGateway(prefixName, zmqTopic, ZmqSocketType.SUB, false, messageSelector,
                transacted);

        open(gateway);

        final TopicSubscriber subscriber = new ZmqTopicSubscriber(gateway, topic, messageSelector, noLocal, exceptionHandler);

        LOGGER.info("Created subscriber: " + subscriber);

        return subscriber;
    }

    @Override
    public MessageConsumer createDurableConsumer(final Topic topic, final String name) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MessageConsumer createDurableConsumer(final Topic topic, final String name, final String messageSelector, final boolean noLocal)
            throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MessageConsumer createSharedConsumer(final Topic topic, final String sharedSubscriptionName) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MessageConsumer createSharedConsumer(final Topic topic, final String sharedSubscriptionName,
        final String messageSelector) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(final Topic topic, final String name) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(final Topic topic, final String name, final String messageSelector) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "ZmqSession [destinationSchema=" + destinationSchema + ", transacted=" + transacted + ", acknowledgeMode=" + acknowledgeMode
                + ", gateways=" + gateways + "]";
    }
}
