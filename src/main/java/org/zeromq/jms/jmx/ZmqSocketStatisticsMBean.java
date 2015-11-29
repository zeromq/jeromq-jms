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
 * ZMQ MBean interface that exposes socket attributes within the JConsole.
 */
public interface ZmqSocketStatisticsMBean extends ZmqMetricsMBean {

    /**
     * @return  return the ZMQ socket name
     */
    String getName();

    /**
     * @return  return the ZMQ socket address
     */
    String getAddr();

    /**
     * @return  return the ZMQ socket type
     */
    String getType();

    /**
     * @return  return the ZMQ socket "bind" indicator
     */
    boolean isBound();

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
