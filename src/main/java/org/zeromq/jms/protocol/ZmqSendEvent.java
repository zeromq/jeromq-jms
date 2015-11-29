package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.jms.ZmqMessage;

/**
 *  Interface defining a SEND event which contains actual context.
 */
public interface ZmqSendEvent extends ZmqHeartbeatEvent {

    /**
     * @return  return the Zero MQ JMS message
     */
    ZmqMessage getMessage();
}
