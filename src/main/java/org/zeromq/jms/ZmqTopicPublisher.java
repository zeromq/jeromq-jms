package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.zeromq.jms.protocol.ZmqGateway;

/**
 * Concrete ZERO MQ implementation of the JMS Topic Publisher.
 */
public class ZmqTopicPublisher extends AbstractZmqMessageProducer implements TopicPublisher {

    /**
     * Construct ZERO MQ topic publisher.
     * @param socketProducer  the socket producer
     * @param topic           the topic
     */
    ZmqTopicPublisher(final ZmqGateway socketProducer, final Topic topic) {
        super(socketProducer, topic);
    }

    @Override
    public Topic getTopic() throws JMSException {

        return (Topic) getDestination();
    }

    @Override
    public void publish(final Message message) throws JMSException {

        send(message);
    }

    @Override
    public void publish(final Topic topic, final Message message) throws JMSException {

        send((Destination) topic, message);
    }

    @Override
    public void publish(final Message message, final int deliveryMode, final int priority, final long timeToLive) throws JMSException {

        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(final Topic topic, final Message message, final int deliveryMode, final int priority, final long timeToLive)
            throws JMSException {

        send((Destination) topic, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public String toString() {
        return "ZmqTopicPublisher [" + "protocol=" + getProtocol() + "]";
    }

}
