package org.zeromq.jms.protocol.filter;

import java.util.Arrays;

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
 * to the DEFAULT value on construction.
 */
@ZmqComponent("fixedTag")
@ZmqUriParameter("filter")
public class ZmqFixedFilterPolicy implements ZmqFilterPolicy {

    public static final String DEFAULT_FILTER = "";

    private String publishTag;
    private String[] subscribeTags;

    /**
     * Construct filter policy using default values.
     */
    public ZmqFixedFilterPolicy() {
        this.publishTag = DEFAULT_FILTER;
        this.subscribeTags = new String[] { DEFAULT_FILTER };
    }

    /**
     * set the ZMQ publish TAG for the ZMQ message.
     * @param tag  the tag
     */
    @ZmqUriParameter("filter.pubTag")
    public void setPublishTags(final String tag) {
        this.publishTag = tag;
    }

    /**
     * set the ZMQ subscription TAGs for the ZMQ socket message filter.
     * @param tags  the tag list
     */
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

	@Override
	public String toString() {
		return "ZmqFixedFilterPolicy [publishTag=" + publishTag + ", subscribeTags=" + Arrays.toString(subscribeTags)
				+ "]";
	}
}
