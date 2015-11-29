package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * Concrete ZERO MQ implementation of the JMS Topic.
 */
public class ZmqTopic extends AbstractZmqDestination implements Topic {

    /**
     * Topic constructor for a given schema lookup name.
     * @param name  the name of the topic
     */
    public ZmqTopic(final String name) {
        super(name);
    }

    /**
     * Topic constructor for a given URI.
     * @param uri  the URI
     */
    public ZmqTopic(final ZmqURI uri) {
        super(uri);
    }

    @Override
    public String getTopicName() throws JMSException {
        return getName();
    }

    @Override
    public String toString() {
        return "ZmqTopic [getName()=" + getName() + "]";
    }
}
