package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.zeromq.jms.protocol.ZmqGateway;

/**
 * Concrete ZERO MQ implementation of the JMS Topic Subscriber.
 */
public class ZmqTopicSubscriber extends AbstractZmqMessageConsumer implements TopicSubscriber {

    private boolean noLocal;

    /**
     * Construct ZERO MQ topic subscriber.
     * @param socketConsumer    the socket consumer
     * @param destination       the destination
     * @param messageSelector   the optional message selector
     * @param noLocal           the noLocal indicator
     * @param exceptionHandler  the exception handler
     */
    ZmqTopicSubscriber(final ZmqGateway socketConsumer, final Destination destination, final String messageSelector, final boolean noLocal,
            final ExceptionListener exceptionHandler) {

        super(socketConsumer, destination, messageSelector, exceptionHandler);

        this.noLocal = noLocal;
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return noLocal;
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic) getDestination();
    }

    @Override
    public String toString() {
        return "ZmqTopicSubscriber [" + "protocol=" + getProtocol() + ", destination=" + getDestination() + ", noLocal=" + noLocal + "]";
    }
}
