package org.zeromq.jms;

import javax.jms.CompletionListener;
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
import javax.jms.MessageProducer;

import org.zeromq.ZMQException;
import org.zeromq.jms.protocol.ZmqGateway;

/**
 * Abstract class Zero MQ JMS message consumer use to subclass for specialisation (Topic and Queue Consumers).
 */
abstract class AbstractZmqMessageProducer implements MessageProducer {

    private ZmqGateway protocol;

    private int deliveryMode;
    private Destination destination;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;

    /**
     * Construct a Zero MQ JMS base message producer (for both topic and queue).
     * @param protocol          the underlying Zero MQ protocol
     * @param destination       the destination
     */
    AbstractZmqMessageProducer(final ZmqGateway protocol, final Destination destination) {

        this.protocol = protocol;
        this.destination = destination;
    }

    /**
     * @return  return the socket producer
     */
    protected ZmqGateway getProtocol() {
        return protocol;
    }

    @Override
    public void close() throws JMSException {

        protocol.close();
    }

    @Override
    public int getDeliveryMode() throws JMSException {

        return deliveryMode;
    }

    @Override
    public Destination getDestination() throws JMSException {

        return destination;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {

        return disableMessageID;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return disableMessageTimestamp;
    }

    @Override
    public int getPriority() throws JMSException {

        return priority;
    }

    @Override
    public long getTimeToLive() throws JMSException {

        return timeToLive;
    }

    @Override
    public void send(final Message message) throws JMSException {

        send(destination, message);
    }

    @Override
    public void send(final Message message, final int deliveryMode, final int priority, final long timeToLive) throws JMSException {

        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(final Destination destination, final Message message, final int deliveryMode, final int priority, final long timeToLive)
            throws JMSException {

        message.setJMSDeliveryMode(deliveryMode);
        message.setJMSPriority(priority);
        message.setJMSExpiration(timeToLive);

        send(destination, message);
    }

    @Override
    public void send(final Destination destination, final Message message) throws JMSException {

        try {
            protocol.send((ZmqMessage) message);
        } catch (ZmqException | ZMQException ex) {
            throw new JMSException(ex.getMessage());
        }
    }

    @Override
    public void setDeliveryMode(final int deliveryMode) throws JMSException {

        this.deliveryMode = deliveryMode;

    }

    @Override
    public void setDisableMessageID(final boolean disableMessageID) throws JMSException {

        this.disableMessageID = disableMessageID;
    }

    @Override
    public void setDisableMessageTimestamp(final boolean disableMessageTimestamp) throws JMSException {

        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    @Override
    public void setPriority(final int priority) throws JMSException {

        this.priority = priority;
    }

    @Override
    public void setTimeToLive(final long timeToLive) throws JMSException {

        this.timeToLive = timeToLive;
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void send(final Message message, final CompletionListener completionListener) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void send(final Destination destination, final Message message, final CompletionListener completionListener) throws JMSException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void send(final Message message, final int deliveryMode, final int priority, final long timeToLive,
        final CompletionListener completionListener) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void send(final Destination destination, final Message message, final int deliveryMode, final int priority, final long timeToLive,
        final CompletionListener completionListener) throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void setDeliveryDelay(final long deliveryDelay) throws JMSException {
        throw new UnsupportedOperationException();
    }
}
