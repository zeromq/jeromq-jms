package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Date;
import java.util.List;

import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;

/**
 * Interface to enable ZERO MQ modification of JMS message delivery acknowledgement.
 *
 * http://www2.imm.dtu.dk/courses/02220/2014/L3/protocols.pdf
 */
public interface ZmqGateway {
    /**
     *  Primary direction of the gateway.
     */
    enum Direction {
        INCOMING, OUTGOING
    };

    /**
     * Bind or Connect the ZMQ socket, etc...
     */
    void open();

    /**
     * @return  return true when socket is open.
     */
    boolean isActive();

    /**
     * Gracefully close the ZMQ socket, etc...
     */
    void close();

    /**
     * Start the protocol listener.
     */
    void start();

    /**
     * Stop the protocol listener.
     */
    void stop();

    /**
     * Commit all messages queue for sending when a transaction is running. Queue messages will
     * send/receive using the Zero MQ "bulk" functionality (i.e. SEND MORE) to ensure multiple
     * messages use the minimum number of frames.
     * @throws ZmqException   throws I/O exception when commit was not successful.
     */
    void commit() throws ZmqException;

    /**
     * Discard all messages queue for sending when a transaction is running. Any new message requested for
     * sending from this point on a transacted socket will be put into a new transaction.
     * @throws ZmqException   throws I/O exception when roll-back was not successful.
     */
    void rollback() throws ZmqException;

    /**
     * Depending on transaction send or store the message.
     * @param message         the message to be sent
     * @throws ZmqException   throws I/O exception on an underlying error
     */
    void send(ZmqMessage message) throws ZmqException;

    /**
     * Attempt to receive a message from the underlying queue, waiting for the specified time before
     * return NULL for nothing retrieved.
     * @param timeout         the wait time out
     * @return                return the message
     * @throws ZmqException   throws I/O exception on an underlying error
     */
    ZmqMessage receive(int timeout) throws ZmqException;

    /**
     * Attempt to receive a message from the underlying queue, waiting indefinitely until something is found.
     * @return                return the message
     * @throws ZmqException   throws I/O exception on an underlying error
     */
    ZmqMessage receive() throws ZmqException;

    /**
     * Register a listener against the protocol for messages and exceptions.
     * @param listener        the listener
     */
    void setListener(ZmqGatewayListener listener);

    /**
     * @return  return the unique protocol name
     */
    String getName();

    /**
     * @return  return the socket address
     */
    String getAddr();

    /**
     * @return  return the socket type
     */
    ZmqSocketType getType();

    /**
     * @return  return true when the socket has been bound to the address
     */
    boolean isBound();

    /**
     * @return  return true when the gateway is part of a transaction
     */
    boolean isTransacted();

    /**
     * @return  return true when the gateway has acknowledgement
     */
    boolean isAcknowledged();

    /**
     * @return  return true when the gateway has a heartbeat
     */
    boolean isHeartbeat();

    /**
     * @return  return the primary direction of the gateway
     */
    Direction getDirection();

    /**
     * @return  return the start date/time of the gateway
     */
    Date getStartTime();

    /**
     * @return  return the metric socket(s) data
     */
    List<ZmqSocketMetrics> getMetrics();
}
