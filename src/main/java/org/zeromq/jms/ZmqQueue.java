package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * Concrete ZERO MQ implementation of the JMS Queue.
 */
public class ZmqQueue extends AbstractZmqDestination implements Queue {

    /**
     * Queue constructor for a given schema lookup name.
     * @param name  the name of the queue
     */
    public ZmqQueue(final String name) {
        super(name);
    }

    /**
     * Queue constructor for a given URI.
     * @param uri  the URI
     */
    public ZmqQueue(final ZmqURI uri) {
        super(uri);
    }

    @Override
    public String getQueueName() throws JMSException {
        return getName();
    }

    @Override
    public String toString() {
        return "ZmqQueue [getName()=" + getName() + "]";
    }

}
