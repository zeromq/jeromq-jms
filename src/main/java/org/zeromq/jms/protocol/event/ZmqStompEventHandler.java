package org.zeromq.jms.protocol.event;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.Format;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.stomp.StompException;
import org.zeromq.jms.stomp.StompMessage;

/**
 * Events based around STOMP messaging.
 */
@ZmqComponent("stomp")
@ZmqUriParameter("event")
public class ZmqStompEventHandler implements ZmqEventHandler {
    private static final Logger LOGGER = Logger.getLogger(ZmqStompEventHandler.class.getCanonicalName());

    private String charset = "UTF-8";
    private Map<String, Format> headerFormats = null;

    /**
     * Set a header name to format conversion.
     * @param headerFormats  the map of format conversion
     */

    public void setHeaderFormats(final Map<String, Format> headerFormats) {
        this.headerFormats = headerFormats;
    }

    /**
     * Set the Byte to/from String charset, i.e. UTF-8, UTF-16, etc... The default is UTF-8.
     * @param charset  the charset, i.e. UTF-8
     */
    public void setCharset(final String charset) {
        this.charset = charset;
    }

    /**
     *  Implementation of the SEND event, the only interface supported by JMS serialisation.
     */
    private abstract class AnstractStompEvent implements ZmqEvent {

        private final ZFrame address;
        private final Object messageId;

        /**
         * Base abstract event.
         * @param address    the address
         * @param messageId  the message ID
         */
        AnstractStompEvent(final ZFrame address, final Object messageId) {
            this.address = address;
            this.messageId = messageId;
        }

        /**
         * @return  return the ZERO MQ socket address
         */
        protected ZFrame getAddress() {
            return address;
        }

        @Override
        public Object getMessageId() {
            return messageId;
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

            AnstractStompEvent other = (AnstractStompEvent) obj;

            if (messageId == null) {
                if (other.messageId != null) {
                    return false;
                }
            } else if (!messageId.equals(other.messageId)) {
                return false;
            }

            return true;
        }
    }

    /**
     *  Implementation of the SEND event, the only interface supported by JMS serialisation.
     */
    private class StompSendEvent extends AnstractStompEvent implements ZmqSendEvent {

        private final ZmqMessage message;

        /**
         * Construct a SEND event.
         * @param address    the ZMQ address
         * @param messageId  the message ID
         * @param message    the content message
         */
        StompSendEvent(final ZFrame address, final Object messageId, final ZmqMessage message) {
            super(address, messageId);

            this.message = message;
        }

        @Override
        public ZmqMessage getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "StompSendEvent [address=" + super.address + ", messageId=" + super.messageId + ", message=" + message + "]";
        }
    }

    /**
     *  Implementation of the SEND (heart-beat) event.
     */
    private class StompHeartbeatEvent extends AnstractStompEvent implements ZmqHeartbeatEvent {

        /**
         * Construct a Heart-beat event.
         * @param address    the ZMQ address
         * @param messageId  the message ID
         */
        StompHeartbeatEvent(final ZFrame address, final Object messageId) {
            super(address, messageId);
        }

        @Override
        public String toString() {
            return "StompHeartbeatEvent [address=" + super.address + ", messageId=" + super.messageId + "]";
        }
    }

    /**
     *  Implementation of the Heart-beat event.
     */
    private class StompAckEvent extends AnstractStompEvent implements ZmqAckEvent {

        /**
         * Construct a ACK event.
         * @param address    the ZMQ address
         * @param messageId  the message ID
         */
        StompAckEvent(final ZFrame address, final Object messageId) {
            super(address, messageId);

        }

        @Override
        public String toString() {
            return "StompAckEvent [address=" + super.address + ", messageId=" + super.messageId + "]";
        }
    }

    /**
     * Convert a Zero MQ JMS message into a STOMP SEND message.
     * @param messageId      the unique message identifier
     * @param message        the JMS message to convert
     * @return               return the STOMP message
     * @throws ZmqException  throw JMS exception
     */
    protected StompMessage convert(final String messageId, final ZmqMessage message) throws ZmqException {
        if (message == null) {
            throw new ZmqException("Cannot convert NULL bodied JMS message to STOMP: " + message);
        }

        final Map<String, String> headers = new HashMap<String, String>();

        try {
            final String body = ((ZmqTextMessage) message).getText();
            final Enumeration<?> nameEnum = message.getPropertyNames();

            while (nameEnum.hasMoreElements()) {
                final String name = (String) nameEnum.nextElement();
                final Object value = message.getObjectProperty(name);

                if (value != null) {
                    if (value instanceof String) {
                        headers.put(name, value.toString());
                    } else if (headerFormats != null) {
                        synchronized (headerFormats) {
                            final Format format = headerFormats.get(name);

                            if (format != null) {
                                final String valueAsStr = format.format(value);
                                headers.put(name, valueAsStr);
                            }
                        }
                    }
                }
            }

            // Add messsage ID when one does not exist
            if (!headers.containsKey(StompMessage.HeaderKey.HEADER_ID.getValue())) {
                headers.put(StompMessage.HeaderKey.HEADER_ID.getValue(), messageId);
            }

            final StompMessage stompMessage = new StompMessage(StompMessage.FrameType.SEND, headers, body);

            return stompMessage;
        } catch (JMSException ex) {
            throw new ZmqException("Cannot convert JMS message to STOMP: " + message, ex);
        }
    }

    /**
     * Convert a STOMP message into a JMS message.
     * @param messsage       the STOMP message
     * @return               return the JMS message
     * @throws ZmqException  throw JMS exception
     */
    protected ZmqMessage convert(final StompMessage messsage) throws ZmqException {
        final ZmqTextMessage zmqMessage = new ZmqTextMessage();
        final String text = messsage.getBody();
        final Map<String, String> headers = messsage.getHeaders();

        try {
            zmqMessage.setText(text);

            for (String name : headers.keySet()) {
                final String value = headers.get(name);

                if (value != null && headerFormats != null) {
                    synchronized (headerFormats) {
                        final Format format = headerFormats.get(name);

                        String valueAsStr;

                        synchronized (headerFormats) {
                            valueAsStr = format.format(value);
                        }
                        zmqMessage.setObjectProperty(name, valueAsStr);
                    }
                } else {
                    zmqMessage.setObjectProperty(name, value);
                }
            }

            return zmqMessage;
        } catch (JMSException ex) {
            throw new ZmqException("Cannot convert STOMP message to JMS: " + messsage, ex);
        }
    }

    @Override
    public ZmqSendEvent createSendEvent(final ZmqMessage message) throws ZmqException {
        final String messageId = UUID.randomUUID().toString();
        ZmqSendEvent event = new StompSendEvent(null, messageId, message);

        return event;
    }

    @Override
    public ZmqAckEvent createAckEvent(final ZmqEvent event) throws ZmqException {
        if (event instanceof AnstractStompEvent) {
            final AnstractStompEvent sendEvent = (AnstractStompEvent) event;
            final ZFrame address = sendEvent.getAddress();
            final Object messageId = sendEvent.getMessageId();

            StompAckEvent ackEvent = new StompAckEvent(address, messageId);

            return ackEvent;
        }

        throw new UnsupportedOperationException("This is not a supported operation.");

    }

    @Override
    public ZmqHeartbeatEvent createHeartbeatEvent() {
        final String messageId = UUID.randomUUID().toString();
        StompHeartbeatEvent event = new StompHeartbeatEvent(null, messageId);

        return event;
    }

    @Override
    public ZMsg createMsg(final ZmqSocketType socketType, final ZmqFilterPolicy filter, final ZmqEvent event) throws ZmqException {

        final ZMsg msg = new ZMsg();
        final String messageId = event.getMessageId().toString();

        StompMessage stompMessage = null;

        if (event instanceof ZmqAckEvent) {
            final StompAckEvent ackEvent = (StompAckEvent) event;
            final ZFrame address = ackEvent.getAddress();

            if (address != null) {
                msg.add(address);
            }

            stompMessage = StompMessage.defineAckMessage(messageId);
        } else if (event instanceof ZmqSendEvent) {
            final StompSendEvent sendEvent = (StompSendEvent) event;
            final ZmqMessage message = sendEvent.getMessage();
            final String messageFilter = (filter == null) ? null : filter.resolve(message);
            final byte[] key = (messageFilter == null) ? null : messageFilter.getBytes();

            if (key != null) {
                msg.add(key);
            }

            stompMessage = convert(messageId, message);
        } else if (event instanceof ZmqHeartbeatEvent) {
            stompMessage = StompMessage.defineSendMessage(messageId, "", "");
        } else {
            throw new UnsupportedOperationException("This is not a supported operation.");
        }

        final String rawMessage = StompMessage.encode(stompMessage);
        try {
            final byte[] data = rawMessage.getBytes(charset);
            msg.add(data);
        } catch (IOException ex) {
            throw new ZmqException("Unable to convert message to and array of bytes: " + stompMessage, ex);
        }

        return msg;
    }

    @Override
    public ZmqEvent createEvent(final ZmqSocketType socketType, final ZMsg msg) throws ZmqException {
        ZFrame address = null;

        switch (socketType) {
        case SUB:
            msg.unwrap(); // remove subscription
            break;

        case ROUTER:
        case REP:
            address = msg.unwrap(); // get address
            break;

        case DEALER:
            // msg.unwrap(); // remove null frame
            break;

        default:
        }

        ZmqEvent event = null;

        final Iterator<ZFrame> msgFrameEnum = msg.iterator();

        while (msgFrameEnum.hasNext()) {
            final ZFrame msgFrame = msgFrameEnum.next();
            final byte[] msgFrameData = msgFrame.getData();

            try {
                final String rawMessage = new String(msgFrameData, charset);
                final StompMessage stompMessage = StompMessage.decode(rawMessage);
                final String messageId = stompMessage.getHeaderValue(StompMessage.HeaderKey.HEADER_ID.getValue());

                StompMessage.FrameType frameType = stompMessage.getFrame();

                switch (frameType) {
                case SEND:
                    final String messageBody = stompMessage.getBody();

                    if (messageBody == null || messageBody.length() == 0) {
                        // heart-beat, and not message content
                        event = new StompHeartbeatEvent(address, messageId);
                    } else {
                        final ZmqMessage message = convert(stompMessage);

                        event = new StompSendEvent(address, messageId, message);
                    }

                    break;

                case ACK:
                    event = new StompAckEvent(address, messageId);

                    break;

                default:
                    LOGGER.log(Level.WARNING, "Received unknown message: " + frameType);
                }
            } catch (ZmqException | StompException | UnsupportedEncodingException ex) {
                throw new ZmqException("Unable to pass ZMQ message", ex);
            }
        }

        return event;
    }
}
