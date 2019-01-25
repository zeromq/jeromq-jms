package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.zeromq.jms.protocol.ZmqGatewayFactory;

/**
 * Generic class for Zero MQ JMS connections for both queues and topics.
 */
public class ZmqConnection implements QueueConnection, TopicConnection {

    private static final Logger LOGGER = Logger.getLogger(ZmqConnection.class.getCanonicalName());

    /**
     *  Handler asynchronous exceptions.
     */
    private class ZmqExceptionHandler implements ExceptionListener {
        private volatile ExceptionListener listener = null;

        @Override
        public void onException(final JMSException exception) {
            final ExceptionListener listener = this.listener;

            try {
                if (listener != null) {
                    listener.onException(exception);
                }
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Unable notify exception listener.", ex);
            }
        }
    }

    private ZmqGatewayFactory gatewayFactory;
    private Map<String, ZmqURI> destinationSchema;
    private ZmqExceptionHandler exceptionHandler = new ZmqExceptionHandler();

    private String clientID = null;

    /**
     * Construct the Zero MQ connection for the given destination UTI.
     * @param gatewayFactory     the gateway factory
     * @param destinationSchema  the map of destination URIs
     */
    public ZmqConnection(final ZmqGatewayFactory gatewayFactory, final Map<String, ZmqURI> destinationSchema) {
        this.gatewayFactory = gatewayFactory;
        this.destinationSchema = destinationSchema;
    }

    @Override
    public void close() throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(final Destination destination, final String messageSelector,
            final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(final Topic topic, final String subscriptionName, final String messageSelector,
            final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
        Session session = new ZmqSession(gatewayFactory, destinationSchema, transacted, acknowledgeMode, exceptionHandler);

        return session;
    }

    @Override
    public String getClientID() throws JMSException {

        return clientID;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {

        return exceptionHandler.listener;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientID(final String clientID) throws JMSException {
        this.clientID = clientID;
    }

    @Override
    public void setExceptionListener(final ExceptionListener listener) throws JMSException {
        this.exceptionHandler.listener = listener;
    }

    @Override
    public void start() throws JMSException {
        //throw new UnsupportedOperationException();
    }

    @Override
    public void stop() throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(final Queue queue, final String messageSelector, final ServerSessionPool sessionPool,
            final int maxMessages) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("resource")
    @Override
    public QueueSession createQueueSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
        QueueSession session = (QueueSession) new ZmqSession(gatewayFactory, destinationSchema, transacted, acknowledgeMode, exceptionHandler);

        return session;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(final Topic topic, final String messageSelector, final ServerSessionPool sessionPool,
            final int maxMessages) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("resource")
    @Override
    public TopicSession createTopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
        TopicSession session = (TopicSession) new ZmqSession(gatewayFactory, destinationSchema, transacted, acknowledgeMode, exceptionHandler);

        return session;
    }

    @Override
    public Session createSession() throws JMSException {
        final Session session = createSession(ZmqSession.AUTO_ACKNOWLEDGE);

        return session;
    }

    @Override
    public Session createSession(final int sessionMode) throws JMSException {
        final Session session = new ZmqSession(gatewayFactory, destinationSchema, false, sessionMode, exceptionHandler);

        return session;
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(final Topic topic, final String subscriptionName, final String messageSelector,
        final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(final Topic topic, final String subscriptionName, final String messageSelector,
            final ServerSessionPool sessionPool, final int maxMessages) throws JMSException {

        throw new UnsupportedOperationException();
    }
}
