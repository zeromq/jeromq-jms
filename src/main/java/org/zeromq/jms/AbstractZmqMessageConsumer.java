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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.zeromq.ZMQException;
import org.zeromq.jms.protocol.ZmqGateway;
import org.zeromq.jms.protocol.ZmqGatewayListener;

/**
 * Abstract class Zero MQ JMS message consumer use to subclass for specialisation (Topic and Queue Consumers).
 */
abstract class AbstractZmqMessageConsumer implements MessageConsumer {

    private final ZmqGateway protocol;
    private final Destination destination;
    private final String messageSelector;
    private final ExceptionListener exceptionHandler;

    private MessageListener listener;

    /**
     * Construct a Zero MQ JMS base message consumer (for both topic and queue).
     * @param protocol          the underlying Zero MQ protocol
     * @param destination       the destination
     * @param messageSelector   the optional JMS message selector
     * @param exceptionHandler  the exception handler
     */
    AbstractZmqMessageConsumer(final ZmqGateway protocol, final Destination destination, final String messageSelector,
            final ExceptionListener exceptionHandler) {

        this.protocol = protocol;
        this.destination = destination;
        this.messageSelector = messageSelector;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * @return  return the socket consumer
     */
    protected ZmqGateway getProtocol() {
        return protocol;
    }

    /**
     * @return  return the destination (queue or topic).
     */
    protected Destination getDestination() {
        return destination;
    }

    @Override
    public void close() throws JMSException {

        protocol.close();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {

        return listener;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return messageSelector;
    }

    @Override
    public Message receive() throws JMSException {

        try {
            return protocol.receive();
        } catch (ZmqException | ZMQException ex) {
            throw new JMSException(ex.getMessage());
        }
    }

    @Override
    public Message receive(final long timeout) throws JMSException {

        try {
            return protocol.receive((int) timeout);
        } catch (ZmqException ex) {
            throw new JMSException(ex.getMessage());
        }
    }

    @Override
    public Message receiveNoWait() throws JMSException {

        try {
            return protocol.receive(0);
        } catch (ZmqException | ZMQException ex) {
            throw new JMSException(ex.getMessage());
        }
    }

    @Override
    public void setMessageListener(final MessageListener listener) throws JMSException {

        this.listener = listener;

        ZmqGatewayListener protcolListener = new ZmqGatewayListener() {

            @Override
            public void onMessage(final ZmqMessage message) {
                listener.onMessage(message);
            }

            @Override
            public void onException(final ZmqException ex) {
                JMSException exception = new JMSException(ex.getMessage());

                exceptionHandler.onException(exception);
            }
        };

        protocol.setListener(protcolListener);
    }
}
