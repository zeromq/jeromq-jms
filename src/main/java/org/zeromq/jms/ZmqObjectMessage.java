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

    @Override
    public Serializable getObject() throws JMSException {
        return object;
    }

    @Override
    public void setObject(final Serializable object) throws JMSException {
        this.object = object;
    }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {	
		super.writeExternal(out);
		
	    out.writeObject(object);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);

		object = (Serializable) in.readObject();
	}
}
