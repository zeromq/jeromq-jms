package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 *  Socket status represents the life cycle states of a socket
 */
public enum ZmqSocketStatus {

    /**
     * Initialising the socket.
     */
    PENDING,
    
    /**
     * Socket is running and listening to messages.
     */
    RUNNING,
    
    /**
     * Socket has been "paused" from consuming or sending JMS message, but will
     * still listening for heart-beats, etc.. This state is obtained when the socket
     * could not bind to a connection, or has lost contact with the server.
     */
    PAUSED,
    
    /**
     * Socket has had an fatal error, and is no-longer responding to any messages.
     */
    ERROR,
    
    /**
     * Socket is is a "stop" state.
     */
    STOPPED;
}
