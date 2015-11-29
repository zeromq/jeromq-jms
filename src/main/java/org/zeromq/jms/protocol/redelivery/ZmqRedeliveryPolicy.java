package org.zeromq.jms.protocol.redelivery;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Collection;

import org.zeromq.jms.protocol.ZmqSendEvent;

/**
 * Re-deliver policy interface to be implemented for specific delivery policies.
 */
public interface ZmqRedeliveryPolicy {

    /**
     * Return the next message to be re-delivered or null when no messages available.
     * @return  return the next message to be re-delivered
     */
    ZmqSendEvent getNextRedeliver();

    /**
     * Mark the following messages for re-deliver or further deliver.
     * @param events  the list of events that failed to be delivered
     */
    void redeliver(Collection<ZmqSendEvent> events);

    /**
     * Mark the following message that were finally delivered successfully.
     * @param events  collection of events that were delivered
     */
    void delivered(Collection<ZmqSendEvent> events);

    /**
     * This message is has been dropped for further re-deliver.
     * @param events     the events to be dropped from further delivery
     */
    void onBackout(final ZmqSendEvent events);
}
