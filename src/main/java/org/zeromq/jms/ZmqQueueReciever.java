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
import javax.jms.Queue;
import javax.jms.QueueReceiver;

import org.zeromq.jms.protocol.ZmqGateway;

/**
 * Concrete ZERO MQ implementation of the JMS Queue Reciever.
 */
public class ZmqQueueReciever extends AbstractZmqMessageConsumer implements QueueReceiver {

    /**
     * Construct ZERO MQ socket receiver.
     * @param socketConsumer    the underlying gateway
     * @param destination       the destination
     * @param messageSelector   the optional JMS message selector
     * @param exceptionHandler  the exception handler
     */
    ZmqQueueReciever(final ZmqGateway socketConsumer, final Destination destination, final String messageSelector,
            final ExceptionListener exceptionHandler) {

        super(socketConsumer, destination, messageSelector, exceptionHandler);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) getDestination();
    }

    @Override
    public String toString() {
        return "ZmqQueueReciever [" + "protocol=" + getProtocol() + ", destination=" + getDestination() + "]";
    }

}
