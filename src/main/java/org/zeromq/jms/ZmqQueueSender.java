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
import javax.jms.Queue;
import javax.jms.QueueSender;

import org.zeromq.jms.protocol.ZmqGateway;

/**
 * Concrete ZERO MQ implementation of the JMS Queue Sender.
 */
public class ZmqQueueSender extends AbstractZmqMessageProducer implements QueueSender {

    /**
     * Construct ZERO MQ socket sender.
     * @param socketProducer  the underlying gateway
     * @param queue           the queue
     */
    ZmqQueueSender(final ZmqGateway socketProducer, final Queue queue) {
        super(socketProducer, queue);
    }

    @Override
    public Queue getQueue() throws JMSException {

        return (Queue) getDestination();
    }

    @Override
    public void send(final Queue queue, final Message message) throws JMSException {
        send((Destination) queue, message);
    }

    @Override
    public void send(final Queue queue, final Message message, final int deliveryMode, final int priority, final long timeToLive)
            throws JMSException {
        send((Destination) queue, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public String toString() {
        return "ZmqQueueSender [" + "protocol=" + getProtocol() + "]";
    }

}
