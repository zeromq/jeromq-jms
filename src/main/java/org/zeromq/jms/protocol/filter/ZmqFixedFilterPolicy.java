package org.zeromq.jms.protocol.filter;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;

/**
 * Simple fixed subscribe policy. ZMQ need a subscribe value and cannot be number, so set
 * to the DEFAUKT value on construction.
 */
@ZmqComponent("fixedFilter")
@ZmqUriParameter("filter")
public class ZmqFixedFilterPolicy implements ZmqFilterPolicy {

    private String producerfilter;
    private String[] consumerFilters;

    /**
     * Construct filter policy using default values.
     */
    public ZmqFixedFilterPolicy() {
        this.producerfilter = ZmqFilterPolicy.DEFAULT_FILTER;
        this.consumerFilters = new String[] { ZmqFilterPolicy.DEFAULT_FILTER };
    }

    @Override
    @ZmqUriParameter("filter.value")
    public void setFilters(final String[] filters) {
        this.producerfilter = (filters == null || filters.length == 0) ? null : filters[0];
        this.consumerFilters = filters;
    }

    @Override
    public String resolve(final ZmqMessage message) {
        return producerfilter;
    }

    @Override
    public String[] getConsumerFilters() {
        return consumerFilters;
    }
}
