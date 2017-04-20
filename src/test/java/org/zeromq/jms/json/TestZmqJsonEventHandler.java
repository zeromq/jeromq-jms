package org.zeromq.jms.json;
/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.UUID;
import java.util.logging.Level;
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
 * Simple google protocol buffer message example.
 */
@ZmqComponent("json")
@ZmqUriParameter("event")
public class TestZmqJsonEventHandler implements ZmqEventHandler {
    private static final Logger LOGGER = Logger.getLogger(TestZmqJsonEventHandler.class.getCanonicalName());

    final ObjectMapper mapper = new ObjectMapper();
    
    /**
     *  Implementation of the SEND event, the only interface supported by JMS serialisation.
     */
    private class ProtobufEvent implements ZmqSendEvent {

        private final Object messageId;
        private final ZmqMessage message;

        /**
         * Construct the JMS based event.
         * @param message  the message
         */
        ProtobufEvent(final ZmqMessage message) {
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

            ProtobufEvent other = (ProtobufEvent) obj;

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
            return "SerialozeEvent [messageId=" + messageId + ", message=" + message + "]";
        }
    }

    @Override
    public ZmqSendEvent createSendEvent(final ZmqMessage message) {
        return createSendEvent(null, message);
    }

    @Override
    public ZmqSendEvent createSendEvent(final Object messageId, final ZmqMessage message) {
        ZmqSendEvent event = new ProtobufEvent(message);

        return event;
    }

    @Override
    public ZmqAckEvent createAckEvent(final ZmqEvent event) {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZmqHeartbeatEvent createHeartbeatEvent() {
        throw new UnsupportedOperationException("This is not a supported operation.");
    }

    @Override
    public ZMsg createMsg(final ZmqSocketType socketType, final ZmqFilterPolicy filter, final ZmqEvent event) throws ZmqException {

        if (event instanceof ZmqSendEvent) {
            final ZmqSendEvent sendEvent = (ZmqSendEvent) event;
            final ZmqTextMessage message = (ZmqTextMessage) sendEvent.getMessage();
            final String messageFilter = (filter == null) ? null : filter.resolve(message);
            final byte[] key = (messageFilter == null) ? null : messageFilter.getBytes();

            final ZMsg msg = new ZMsg();

            if (key != null) {
                msg.add(key);
            }

            try {
                final String messageId = UUID.randomUUID().toString();
                final String body = message.getText();
                final TestJsonMessage jsonMessage = new TestJsonMessage();
                
                jsonMessage.setMessageId(messageId);
                jsonMessage.setMessageType(TestJsonMessage.MessageType.SENT);
                jsonMessage.setSentDate(new Date());
                jsonMessage.setBody(body);

                final byte[] data = mapper.writeValueAsBytes(jsonMessage);

                msg.add(data);
            } catch (JMSException | JsonProcessingException ex) {
                throw new ZmqException("Unable to convert message to and array of bytes: " + message, ex);
            }

            return msg;
        }

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
                final byte[] msgFrameData = msgFrames.getLast().getData();
                final TestJsonMessage jsonMessage = mapper.readValue(msgFrameData, TestJsonMessage.class);
                final ZmqTextMessage message = new ZmqTextMessage();

                message.setJMSMessageID(jsonMessage.getMessageId());
                message.setJMSDeliveryTime(jsonMessage.getSentDate().getTime());
                message.setText(jsonMessage.getBody());

                final ZmqEvent sendEvent = new ProtobufEvent(message);

                return sendEvent;
            } catch (JMSException | IOException ex) {
                LOGGER.log(Level.SEVERE, "Unable to create event from ZMQ message?", ex);
                throw new ZmqException("Unable to create event from ZMQ message?", ex);
            }
        }

        throw new ZmqException("Unable to creae event fomr the ZMQ message");
    }
}
