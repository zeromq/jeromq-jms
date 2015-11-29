package org.zeromq.jms.protocol.event;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.ZMsg;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.protocol.ZmqAckEvent;
import org.zeromq.jms.protocol.ZmqEvent;
import org.zeromq.jms.protocol.ZmqHeartbeatEvent;
import org.zeromq.jms.protocol.ZmqSendEvent;
import org.zeromq.jms.protocol.ZmqSocketType;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;

/**
 * Interface for ZMQ event handler.
 */
public interface ZmqEventHandler {

    /**
     * Return the SEND event based on the JMS message.
     * @param  message        the JMS message
     * @return                return the SEND event
     * @throws ZmqException   throw JMS exception on failure
     */
    ZmqSendEvent createSendEvent(ZmqMessage message) throws ZmqException;

    /**
     * Return the ACK event based on the event.
     * @param event           the event
     * @return                return the receipt event
     * @throws ZmqException   throw JMS exception on failure
     */
    ZmqAckEvent createAckEvent(ZmqEvent event) throws ZmqException;

    /**
     * Return the HEARBEAT event.
     * @return             return the heart-beat event
     */
    ZmqHeartbeatEvent createHeartbeatEvent();

    /**
     * Return a ZERO MQ message based on the socket type and event.
     * @param socketType      the socket type, i.e. PUB, REQ, etc...
     * @param filter          the filter policy
     * @param event           the event
     * @return                return the ZERO MQ message
     * @throws ZmqException   throw JMS exception on failure
     */
    ZMsg createMsg(ZmqSocketType socketType, ZmqFilterPolicy filter, ZmqEvent event) throws ZmqException;

    /**
     * Return a event based on the ZERO MQ message and socket type.
     * @param socketType      the socket type, i.e. PUB, REQ, etc...
     * @param msg             the ZERO MQ message
     * @return                return the event
     * @throws ZmqException   throw JMS exception on failure
     */
    ZmqEvent createEvent(ZmqSocketType socketType, ZMsg msg) throws ZmqException;
}
