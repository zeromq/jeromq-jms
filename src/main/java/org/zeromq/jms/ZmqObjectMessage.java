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
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

/**
 * Zero MQ implementation of a JMS Object Message.
 */
public class ZmqObjectMessage extends ZmqMessage implements ObjectMessage {

    private Serializable object;

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getBody(final Class<T> c) throws JMSException {
        return (T) object;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean isBodyAssignableTo(final Class c) throws JMSException {
        return c.isAssignableFrom(Serializable.class);
    }

    @Override
    public Serializable getObject() throws JMSException {
        return object;
    }

    @Override
    public void setObject(final Serializable object) throws JMSException {
        this.object = object;
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(object);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        object = (Serializable) in.readObject();
    }
}
