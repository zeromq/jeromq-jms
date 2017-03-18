package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.JMSException;

/**
 *  Helper class to build ZMQ JMS text messages.
 */
public class ZmqTextMessageBuilder {

    private final ZmqTextMessage message;

    /**
     * Construct the builder around a JMS text message.
     */
    ZmqTextMessageBuilder() {
        message = new ZmqTextMessage();
    }

    /**
     * Create an instance of the Zero MQ text message builder.
     * @return               return builder
     * @throws JMSException  throws JMS exception
     */
    public static ZmqTextMessageBuilder create() throws JMSException {
        return new ZmqTextMessageBuilder();
    }

    public ZmqTextMessageBuilder appendJmsMessageId(final String id) throws JMSException {
        message.setJMSMessageID(id);

        return this;
    } 

    /**
     * Set the text of the message.
     * @param  text          the text
     * @return               return builder
     * @throws JMSException  throws JMS exception
     */
    public ZmqTextMessageBuilder appendText(final String text) throws JMSException {
        message.setText(text);

        return this;
    }

    /**
     * Add the specified string property to the message.
     * @param name           the property name
     * @param value          the property value
     * @return               return builder
     * @throws JMSException  throws JMS exception
     */
    public ZmqTextMessageBuilder appendProperty(final String name, final String value) throws JMSException {
        message.setStringProperty(name, value);

        return this;
    }

    /**
     * @return  return the text message
     */
    public ZmqTextMessage toMessage() {

        return message;
    }
}
