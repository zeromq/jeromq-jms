package org.zeromq.jms.text;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.logging.Level;
/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.logging.Logger;

import javax.jms.JMSException;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.ZmqTextMessage;
import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;
import org.zeromq.jms.protocol.ZmqAckEvent;
import org.zeromq.jms.protocol.ZmqEvent;
import org.zeromq.jms.protocol.ZmqHeartbeatEvent;
import org.zeromq.jms.protocol.ZmqSendEvent;
import org.zeromq.jms.protocol.ZmqSocketType;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;

/**
 * Reads a text message from external ZMQ that is not JMS.
 */
@ZmqComponent("text")
@ZmqUriParameter("event")
public class TestZmqTextEventHandler implements ZmqEventHandler {
    private static final Logger LOGGER = Logger.getLogger(TestZmqTextEventHandler.class.getCanonicalName());

    /*
     * Default character encoding.
     */
    private String charset = "UTF-8";

    /**
     * Text events generate by the subscriber.
     */
    private class TextEvent implements ZmqSendEvent {

        private final Object messageId;
        private final ZmqMessage message;

        /**
         * Construct the JMS based event.
         * @param message  the message
         */
        TextEvent(final ZmqMessage message) {
            this.messageId = null;
            this.message = message;
        }

        @Override
        public Object getMessageId() {
            return messageId;
        }

        @Override
        public ZmqMessage getMessage() {
            return message;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            TextEvent other = (TextEvent) obj;

            if (messageId == null) {
                if (other.messageId != null) {
                    return false;
                }
            } else if (!messageId.equals(other.messageId)) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return "TextEvent [messageId=" + messageId + ", message=" + message + "]";
        }
    }

    /**
     * Set the Byte to/from String charset, i.e. UTF-8, UTF-16, etc... The default is UTF-8.
     * @param charset  the charset, i.e. UTF-8
     */
    public void setCharset(final String charset) {
        this.charset = charset;
    }

    @Override
    public ZmqSendEvent createSendEvent(final ZmqMessage message) throws ZmqException {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZmqSendEvent createSendEvent(final Object messageId, final ZmqMessage message) throws ZmqException {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZmqAckEvent createAckEvent(final ZmqEvent event) throws ZmqException {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZmqHeartbeatEvent createHeartbeatEvent() {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZMsg createMsg(final ZmqSocketType socketType, final ZmqFilterPolicy filter, final ZmqEvent event) throws ZmqException {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZmqEvent createEvent(final ZmqSocketType socketType, final ZMsg msg) throws ZmqException {
        if (msg.contentSize() == 0) {
            return null;
        }

        LinkedList<ZFrame> msgFrames = new LinkedList<ZFrame>();
        ZFrame msgFrame = msg.pop();

        // Get all address hops and actual data message
        while (msgFrame != null) {
            if (msgFrame.hasData()) {
                msgFrames.add(msgFrame);
            }
            msgFrame = msg.pop();
        }

        // The last is always the data messages, others are optional address hops
        // see:  http://zguide.zeromq.org/php:chapter3
        if (msgFrames.size() > 0) {

            try {
                final String type = new String(msgFrames.getFirst().getData(), charset);
                final String body = new String(msgFrames.getLast().getData(), charset);
                final ZmqTextMessage message = new ZmqTextMessage();

                message.setJMSType(type);
                message.setText(body);

                final ZmqEvent sendEvent = new TextEvent(message);

                return sendEvent;
            } catch (JMSException | UnsupportedEncodingException ex) {
                LOGGER.log(Level.SEVERE, "Unable to create event from ZMQ message?", ex);
                throw new ZmqException("Unable to create event from ZMQ message?", ex);
            }
        }

        throw new ZmqException("Unable to creae event fomr the ZMQ message");
    }
}
