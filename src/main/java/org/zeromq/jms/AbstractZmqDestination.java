package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.Destination;

/**
 * Abstract class Zero MQ JMS destination.
 */
public abstract class AbstractZmqDestination implements Destination {

    private final String name;
    private final ZmqURI uri;

    /**
     * Create a Zero MQ destination with a given name.
     * @param name  the name
     */
    public AbstractZmqDestination(final String name) {
        this.name = name;
        this.uri = null;
    }

    /**
     * Create a Zero MQ destination with a given URI.
     * @param uri  the URI
     */
    public AbstractZmqDestination(final ZmqURI uri) {
        this.name = uri.getDestinationName();
        this.uri = uri;
    }

    /**
     * @return  return the name of the destination
     */
    public String getName() {
        return name;
    }

    /**
     * @return  return the optional URI of the destination
     */
    public ZmqURI getURI() {
        return uri;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        AbstractZmqDestination other = (AbstractZmqDestination) obj;

        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }

        return true;
    }
}
