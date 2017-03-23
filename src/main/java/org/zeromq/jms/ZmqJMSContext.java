package org.zeromq.jms;

/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * Generic class to implement the JMS 2.0 JMSContext interface.
 */
public class ZmqJMSContext implements JMSContext {

    private final ZmqConnection connection;
    private final int sessionMode;
    private final ZmqSession session;

    private boolean closed;
    private boolean autoStart;
    private MessageProducer innerProducer;

    /**
     * Construct the XMQ JMSContext Implementation.
     * @param connection   the connection
     * @param sessionMode  the session mode
     */
    public ZmqJMSContext(final ZmqConnection connection, final int sessionMode) {
        this.connection = connection;
        this.sessionMode = sessionMode;

        try {
            this.session = (ZmqSession) connection.createSession(sessionMode);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }

        closed = false;
        autoStart = false;
    }

    /**
     * Check the "AutoStart" state.
     */
    protected void checkAutoStart() {
        if (closed) {
            throw new IllegalStateRuntimeException("Context is closed");
        }

        if (autoStart) {
            try {
                connection.start();
            } catch (JMSException ex) {
                throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
            }
        }
    }

    /**
     * Check the session state.
     */
    protected void checkSession() {
        if (closed) {
            throw new IllegalStateRuntimeException("Context is closed");
        }
    }

    @Override
    public void acknowledge() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void close() {
        closed = false;

        try {
            session.close();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public synchronized void commit() {
        try {
            session.commit();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue) {
        throw  new UnsupportedOperationException();
    }

    @Override
    public QueueBrowser createBrowser(final Queue queue, final String messageSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesMessage createBytesMessage() {
        return new ZmqByteMessage();
    }

    @Override
    public JMSConsumer createConsumer(final Destination destination) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createConsumer(destination);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createConsumer(final Destination destination, final String messageSelector) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createConsumer(destination, messageSelector);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createConsumer(final Destination destination, final String messageSelector, final boolean noLocal) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createConsumer(destination, messageSelector, noLocal);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSContext createContext(final int sessionMode) {
        final JMSContext context = new ZmqJMSContext(connection, sessionMode);

        return context;
    }

    @Override
    public JMSConsumer createDurableConsumer(final Topic topic, final String name) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createDurableConsumer(topic, name);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(final Topic topic, final String name, final String messageSelector, final boolean noLocal) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createDurableConsumer(topic, name, messageSelector, noLocal);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        checkSession();

        try {
             return session.createMapMessage();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Message createMessage() {
        checkSession();

        try {
             return session.createMessage();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        checkSession();

        try {
             return session.createObjectMessage();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public ObjectMessage createObjectMessage(final Serializable object) {
        checkSession();

        try {
             return session.createObjectMessage(object);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer createProducer() {
        checkSession();

        try {
            if (innerProducer == null) {
                synchronized (this) {
                    if (innerProducer == null) {
                        innerProducer = session.createProducer(null);
                    }
                }
            }

           JMSProducer producer = new ZmqJMSProducer(this, innerProducer);

            return producer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Queue createQueue(final String queue) {
        checkSession();

        try {
             return session.createQueue(queue);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(final Topic topic, final String name) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createSharedConsumer(topic, name);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(final Topic topic, final String name, final String messageSelector) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createSharedConsumer(topic, name, messageSelector);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(final Topic topic, final String sharedSubscriptionName) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createSharedDurableConsumer(topic, sharedSubscriptionName);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(final Topic topic, final String sharedSubscriptionName, final String messageSelector) {
        checkSession();

        try {
             final MessageConsumer messageConsumer = session.createSharedDurableConsumer(topic, sharedSubscriptionName, messageSelector);
             final JMSConsumer jmsConsumer = new ZmqJMSConsumer(this, messageConsumer);

             checkAutoStart();

             return jmsConsumer;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        checkSession();

        try {
             return session.createStreamMessage();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        checkSession();

        try {
             return session.createTemporaryQueue();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        checkSession();

        try {
             return session.createTemporaryTopic();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        checkSession();

        try {
             return session.createTextMessage();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public TextMessage createTextMessage(final String text) {
        checkSession();

        try {
             return session.createTextMessage(text);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Topic createTopic(final String topicName) {
        checkSession();

        try {
             return session.createTopic(topicName);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    @Override
    public String getClientID() {
        try {
            return connection.getClientID();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public ExceptionListener getExceptionListener() {
        try {
           return connection.getExceptionListener();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() {
        try {
            return connection.getMetaData();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public boolean getTransacted() {
        checkSession();

        try {
            return session.getTransacted();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void recover() {
        checkSession();

        try {
            session.recover();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void rollback() {
        try {
            session.rollback();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void setAutoStart(final boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public void setClientID(final String clientID) {
        try {
            connection.setClientID(clientID);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void setExceptionListener(final ExceptionListener listener) {
        try {
            connection.setExceptionListener(listener);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void start() {
        try {
            connection.start();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void stop() {
        try {
            connection.stop();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void unsubscribe(final String name) {
        try {
            session.unsubscribe(name);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public String toString() {
        return "ZmqContext [connection=" + connection + ", sessionMode=" + sessionMode + ", session=" + session
                + ", closed=" + closed + "]";
    }
}
