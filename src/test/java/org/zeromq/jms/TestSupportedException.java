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
import javax.jms.Queue;
import javax.jms.Topic;

import org.junit.Test;

/**
 * Test the ZMQ exception.
 */
public class TestSupportedException {

    /**
     * Test JMS connection factory failure.
     * @throws JMSException  throws JMS exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testZmqConnectionFactory() throws JMSException {
        final ZmqConnectionFactory factory = new ZmqConnectionFactory();

        factory.createConnection(null, null);
        factory.createTopicConnection(null, null);
    }

    /**
     * Test JMS connection failure.
     * @throws JMSException  throws JMS exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testZmqConnection() throws JMSException {
        final ZmqConnection connection = new ZmqConnection(null, null);

        connection.close();
        connection.createConnectionConsumer((Destination) null, null, null, 0);
        connection.createDurableConnectionConsumer(null, null, null, null, 0);
        connection.getClientID();
        connection.getExceptionListener();
        connection.getMetaData();
        connection.setClientID(null);
        connection.stop();
        connection.createConnectionConsumer((Queue) null, null, null, 0);
        connection.createDurableConnectionConsumer((Topic) null, null, null, null, 0);
    }

    /**
     * Test JMS session failure.
     * @throws JMSException  throws JMS exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testZmqSession() throws JMSException {
        final ZmqSession session = new ZmqSession(null, null, false, -1, null);

        session.createDurableSubscriber(null, null);
        session.createDurableSubscriber(null, null, null, false);
        session.createMapMessage();
        session.createMessage();
        session.createObjectMessage();
        session.createObjectMessage(null);
        session.createStreamMessage();
        session.createTemporaryTopic();

        session.run();
        session.setMessageListener(null);
        session.unsubscribe(null);
        session.createBrowser(null);
        session.createBrowser(null, null);
        session.createTemporaryQueue();

    }
}
