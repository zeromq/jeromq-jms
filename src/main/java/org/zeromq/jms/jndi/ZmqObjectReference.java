package org.zeromq.jms.jndi;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;

/**
 * Zero MQ JNI object reference implementation to enable JNDI lookups of Zero MQ factories and
 * destinations (i.e. Queues and Topics).
 */
public class ZmqObjectReference implements Referenceable {

    private final String name;

    /**
     * Construct ZMQ object reference.
     * @param name  the parameter
     */
    public ZmqObjectReference(final String name) {

        this.name = name;
    }

    @Override
    public Reference getReference() throws NamingException {
        final Reference ref = new Reference(name, ZmqObjectFactory.class.getName(), null);

        return ref;
    }
}
