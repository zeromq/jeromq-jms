package org.zeromq.jms.jmx;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Date;

/**
 * ZMQ MBean interface that exposes gateway attributes and operations within the JConsole.
 */
public interface ZmqGatewayManagerMBean extends ZmqMetricsMBean {

    /**
     * @return  return the given name of the gateway
     */
    String getName();

    /**
     * @return  return the start time of the socket
     */
    Date getStartTime();

    /**
     * @return  return the transaction indicator flag
     */
    boolean isTransacted();

    /**
     * @return  return the bound indicator flag
     */
    boolean isBound();

    /**
     * @return  return the acknowledged indicator flag
     */
    boolean isAcknowledged();

    /**
     * @return  return the heartbeat indicator flag
     */
    boolean isHeartbeat();

    /**
     * @return  return the principal direction of the gateway, i.e. incoming or outgoing
     */
    String getDirection();

    /**
     * @return return the total sent count.
     */
    long getSendCount();

    /**
     * @return  return the last time message was sent
     */
    Date getLastSendTime();

    /**
     * @return return the total received count.
     */
    long getReceiveCount();

    /**
     * @return  return the last time message was received
     */
    Date getLastReceiveTime();

}
