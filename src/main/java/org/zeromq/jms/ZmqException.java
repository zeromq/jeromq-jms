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
 *  Zero MQ JMS exception.
 */
public class ZmqException extends JMSException {

    private static final long serialVersionUID = 1661699719108939224L;

    /**
     * Constructs a new exception with the specified detail message.
     * @param message  the message detail
     */
    public ZmqException(final String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     * @param message  the message detail
     * @param cause    the cause
     */
    public ZmqException(final String message, final Exception cause) {
        super(message);
        setLinkedException(cause);
    }
}
