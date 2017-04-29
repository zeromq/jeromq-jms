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
@ZmqComponent("fixedTag")
@ZmqUriParameter("filter")
public class ZmqFixedFilterPolicy implements ZmqFilterPolicy {

    private String publishTag;
    private String[] subscribeTags;

    /**
     * Construct filter policy using default values.
     */
    public ZmqFixedFilterPolicy() {
        this.publishTag = ZmqFilterPolicy.DEFAULT_FILTER;
        this.subscribeTags = new String[] { ZmqFilterPolicy.DEFAULT_FILTER };
    }

    @ZmqUriParameter("filter.pubTag")
    public void setPublishTags(final String tag) {
        this.publishTag = tag;
    }

    @ZmqUriParameter("filter.subTags")
    public void setSubscribeTags(final String[] tags) {
        this.subscribeTags = tags;
    }

    @Override
    public String resolve(final ZmqMessage message) {
        return publishTag;
    }

    @Override
    public String[] getSubscirbeTags() {
        return subscribeTags;
    }
}
