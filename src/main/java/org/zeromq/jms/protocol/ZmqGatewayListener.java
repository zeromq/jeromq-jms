package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;

/**
 * The protocol listener is used to receive asynchronous delivery of messages and exceptions.
 */
public interface ZmqGatewayListener {

    /**
     * Pass the message to the listener.
     * @param message  the JMS message
     */
    void onMessage(ZmqMessage message);

    /**
     * Pass the exception to the listener.
     * @param ex       the exception
     */
    void onException(ZmqException ex);
}
