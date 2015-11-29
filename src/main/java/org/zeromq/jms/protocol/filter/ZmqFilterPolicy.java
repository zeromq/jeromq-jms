package org.zeromq.jms.protocol.filter;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.jms.ZmqMessage;

/**
 * Interface to enable ZERO MQ modification of subscriber filters based on messages.
 */
public interface ZmqFilterPolicy {

    String DEFAULT_FILTER = "none";

    /**
     * Return the filter to be used, given the specified message.
     * @param message   the message used to determine the filter
     * @return          return the ZMQ filter
     */
    String resolve(ZmqMessage message);

    /**
     * Return the consumer filters string used in subscription.
     * @return          return the filters used in the subscription
     */
    String[] getConsumerFilters();

    /**
     * Optional ZMQ filter values setter.
     * @param filters  the list of filter strings, or null
     */
    void setFilters(String[] filters);
}
