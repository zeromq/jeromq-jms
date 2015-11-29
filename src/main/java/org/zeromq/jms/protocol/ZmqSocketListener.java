package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * Socket listener interface that is used by the gateway to perform addition functionality related to
 * the socket operation and related Zero MQ MSG.
 */
public interface ZmqSocketListener {

    /**
     * This function is called when the specified source socket fails to be send/receive.
     * @param  source  the socket session involved
     * @param  event    the event that has failed
     */
    void error(ZmqSocketSession source, ZmqEvent event);

    /**
     * Invoked by the socket session when the connection has been open successfully.
     * @param  source  the socket session involved
     */
    void open(ZmqSocketSession source);

    /**
     * Poll for the next outgoing event to send to the specified source. This is general a message event
     * within the outgoing queue, but this could be a heart-beat, etc...
     * @param  source  the socket session involved
     * @return         return the polled event, or null for no more events
     */
    ZmqEvent send(ZmqSocketSession source);

    /**
     * This function is called when a messaged is POLLED from the incoming queue. An optional return
     * Response Event can be returned to be sent back to the sending address.
     * @param  source  the socket session involved
     * @param  event   the event generated from consuming a Zero MQ MSG message
     * @return         return an optional response event or NULL for nothing to do.
     */
    ZmqEvent receive(ZmqSocketSession source, ZmqEvent event);

    /**
     * Invoked by the socket session when the connection has been closed successfully.
     * @param  source  the socket session involved
     */
    void close(ZmqSocketSession source);
}
