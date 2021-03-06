package org.zeromq.jms.protocol.filter;
/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;

import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;

/**
 * This XMQ filter is has fixed subscription tags, but obtains the publish tag from the
 * JMS header properties.
 */
@ZmqComponent("propertyTag")
@ZmqUriParameter("filter")
public class ZmqJmsPropertyFilterPolicy implements ZmqFilterPolicy {

    private static final Logger LOGGER = Logger.getLogger(ZmqJmsPropertyFilterPolicy.class.getCanonicalName());

    private String[] consumerTags = null;
    private String propertyName = null;

    /**
     * Set the property name that contains the TAG value to publish with ZMQ message.
     * @param propertyName  the property name within the message header
     */
    @ZmqUriParameter("filter.pubPropertyName")
    public void setPublishTag(final String propertyName) {
        this.propertyName = propertyName;
    }

    /**
     * Set the subscription TAGs values for the ZMQ socket to filter against.
     * @param consumerTags  the list of tags
     */
    @ZmqUriParameter("filter.subTags")
    public void setSubscribeTags(final String[] consumerTags) {
        this.consumerTags = consumerTags;
    }

    @Override
    public String resolve(final ZmqMessage message) {
        try {
            final Object value = message.getObjectProperty(propertyName);

            if (value == null) {
               return null;
            }

           return value.toString();
        } catch (JMSException ex) {
            LOGGER.log(Level.WARNING, "Messgae property [" + propertyName + "] lookup failed.", ex);
            return null;
        }
    }

    @Override
    public String[] getSubscirbeTags() {
        return consumerTags;
    }

    @Override
    public String toString() {
        return "ZmqJmsPropertyFilterPolicy [consumerTags=" + Arrays.toString(consumerTags) + ", propertyName="
            + propertyName + "]";
    }
}
