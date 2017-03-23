package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * Zero MQ implementation of a JMS Text Message.
 */
public class ZmqTextMessage extends ZmqMessage implements TextMessage {

    private String text;

    @Override
    public String getText() throws JMSException {
        return text;
    }

    @Override
    public void setText(final String text) throws JMSException {
        this.text = text;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getBody(final Class<T> c) throws JMSException {
        return (T) text;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean isBodyAssignableTo(final Class c) throws JMSException {
        return c.isAssignableFrom(String.class);
    }

    @Override
    public String toString() {
        try {
            return "ZmqTextMessage [text=" + text + ", getProperties()=" + getProperties() + ", getJMSCorrelationID()=" + getJMSCorrelationID()
                    + ", getJMSCorrelationIDAsBytes()=" + getJMSCorrelationIDAsBytes() + ", getJMSDeliveryMode()=" + getJMSDeliveryMode()
                    + ", getJMSDestination()=" + getJMSDestination() + ", getJMSExpiration()=" + getJMSExpiration() + ", getJMSMessageID()="
                    + getJMSMessageID() + ", getJMSPriority()=" + getJMSPriority() + ", getJMSRedelivered()=" + getJMSRedelivered()
                    + ", getJMSReplyTo()=" + getJMSReplyTo() + ", getJMSTimestamp()=" + getJMSTimestamp() + ", getJMSType()=" + getJMSType()
                    + ", getPropertyNames()=" + getPropertyNames() + ", toString()=" + super.toString() + ", getClass()=" + getClass()
                    + ", hashCode()=" + hashCode() + "]";
        } catch (JMSException ex) {
            return ex.getMessage();
        }
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(text);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        text = (String) in.readObject();
    }
}
