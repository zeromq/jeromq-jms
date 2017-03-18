package org.zeromq.jms.stomp;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * STOMP message implementation (https://stomp.github.io).
 */
public class StompMessage implements Externalizable {

    /**
     *  STOMP frame types as an enumerator.
     */
    public enum FrameType {
        // Client Frames
        SEND, SUBSCRIBE, UNSUBSCRIBE, BEGIN, COMMIT, ABORT, ACK, NACK, DISCONNECT,

        // Stomp Frame
        CONNECT, CONNECTED,

        // Server Frame
        MESSAGE, RECEIPT, ERROR
    };

    /**
     * STOMP parameters as an enumerator.
     */
    public enum HeaderKey {
        /**
         * The accept-version.
         */
        HEADER_ACCEPT_VERSION("accept-version"),

        /**
         * The version.
         */
        HEADER_VERSION("version"),

        /**
         * The host.
         */
        HEADER_HOST("host"),

        /**
         * The destination.
         */
        HEADER_DESTINATION("destination"),

        /**
         * Contains "auto", "client", or "client-individual", Default "auto" tell server if the
         * client is acknowledge published messages.
         */
        HEADER_ACK("ack"),

        /**
         * The Heart-beating settings.
         */
        HEADER_HEADER_BEAT("heart-beat"),

        /**
         * Contains information about the STOMP server i.e. name ["/" version] *(comment), i.e. ZeroMQ/0.3.5
         */
        HEADER_SERVER("server"),

        /**
         * Contains the message body mime type, i.e. test/html, text/plain, etc..
         */
        HEADER_CONTENT_TYPE("content-type"),

        /**
         * Contains the length of the message body.
         */
        HEADER_CONTENT_LENGTH("content-length"),

        /**
         * Contains the subscription identifier of the message, that is part of the message frame.
         */
        HEADER_ID("id"),

        /**
         * Contains the unique identifier of the message, that is part of the message frame.
         */
        HEADER_MESSAGE_ID("message-id"),

        /**
         * Contains the "message-id" that was received and sent back in a RECEIPT frame.
         */
        HEADER_RECEIPT_ID("receipt-id"),

        /**
         * Contains the "receipt" message an arbitrary value returned as receipt.
         */
        HEADER_RECEIPT("receipt"),

        /**
         * Contains the optional error short description for ERRO frames, i.e. mailformed frame received.
         */
        HEADER_MESSAGE("messsage");

        private String value;

        /**
         * Construct STOMP header key.
         * @param value  the value
         */
        HeaderKey(final String value) {
            this.value = value;
        }

        /**
         * @return  return the header key value.
         */
        public String getValue() {
            return value;
        }

        private static final Map<String, HeaderKey> VALUE_MAP;

        static {
            final Map<String, HeaderKey> valueMap = new HashMap<String, HeaderKey>();

            for (final HeaderKey en : HeaderKey.values()) {
                valueMap.put(en.value, en);
            }

            VALUE_MAP = Collections.unmodifiableMap(valueMap);
        }

        /**
         * Return the header key as an enumerator based on the specified value.
         * @param value   the string representation of the value
         * @return        return the enumerator key value
         */
        public static HeaderKey getEnum(final String value) {
            if (!VALUE_MAP.containsKey(value)) {
                throw new IllegalArgumentException("Unknown value: " + value);
            }
            return VALUE_MAP.get(value);
        }
    }

    public static final String LINE_SPERATOR = System.getProperties().getProperty("line.separator");
    public static final String NULL_OCTET = new String(new byte[] { 0 });

    private FrameType frame;
    private Map<String, String> headers;
    private String body;

    /**
     * Constructor ONLY required for Externaliable interface.
     */
    public StompMessage() {
    	
    }
    
    /**
     * Construct a default STOMP message.
     * @param frame    the frame
     * @param headers  the headers
     * @param body     the body
     */
    public StompMessage(final FrameType frame, final Map<String, String> headers, final String body) {
        this.frame = frame;
        this.headers = headers;
        this.body = body;
    }

    /**
     * @return  return the message frame, i.e. CONNECT, CONNECTED, etc....
     */
    public FrameType getFrame() {
        return frame;
    }

    /**
     * @return  return header property containing the "destination" were the message is to be sent, i.e. topic/a
     */
    public String getDestination() {
        return headers.get(HeaderKey.HEADER_DESTINATION.getValue());
    }

    /**
     * @return  return the header property for "content-type", i.e. text/plain.
     */
    public String getContentType() {
        return headers.get(HeaderKey.HEADER_CONTENT_TYPE.getValue());
    }

    /**
     * @return  return the header contents
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Return the header value for the specified key. Return NULL if the header is not found.
     * @param key  the key
     * @return     return the value, or null
     */
    public String getHeaderValue(final String key) {
        return headers.get(key);
    }

    /**
     * Return the header value for the specified key. Return NULL if the header is not found.
     * @param key  the key
     * @return     return the value, or null
     */
    public String getHeaderValue(final HeaderKey key) {
        return headers.get(key.getValue());
    }

    /**
     * Return the header value for the specified key as an "Integer". Return NULL if the header is not found.
     * @param key  the key
     * @return     return the "Integer" value, or null
     */
    public Integer getHeaderValueAsInteger(final HeaderKey key) {
        final String value = headers.get(key.getValue());

        if (value == null || value.length() == 0) {
            return null;
        }

        final int valueAsInt = Integer.parseInt(value);

        return valueAsInt;
    }

    /**
     * Return the header value, or the specified default value when it does not exist.
     * @param key           the key
     * @param defaultValue  the default value
     * @return              return the value, or default value
     */
    public String getHeaderValue(final HeaderKey key, final String defaultValue) {
        String value = headers.get(key.getValue());

        if (value == null) {
            return defaultValue;
        }

        return value;
    }

    /**
     * @return  return the body of the STOMP message
     */
    public String getBody() {
        return body;
    }

    /**
     * @return  return the encoded STOMP  message.
     */
    public String encode() {
        return encode(this);
    }

    /**
    * Encode the message into STOMP format.
    * @param message  the message to be encoded
    * @return         return the encoded message
    */
    public static String encode(final StompMessage message) {
        StringBuilder messageBuffer = new StringBuilder();

        messageBuffer.append(message.getFrame()).append(LINE_SPERATOR);
        Map<String, String> headers = message.getHeaders();

        for (String key : headers.keySet()) {
            String value = headers.get(key);

            messageBuffer.append(key).append(":").append(value).append(LINE_SPERATOR);
        }

        if (message.getBody() != null) {
            messageBuffer.append(LINE_SPERATOR);
            messageBuffer.append(message.getBody());
        }

        messageBuffer.append(NULL_OCTET).append(LINE_SPERATOR);

        return messageBuffer.toString();
    }

    /**
     * Decode the STOMP message into a message object.
     * @param message          the message to decode
     * @return                 return the decoded message
     * @throws StompException  throws exception when message cannot be decoded
     */
    public static StompMessage decode(final String message) throws StompException {
        int index = message.indexOf('\n');

        if (index == -1) {
            throw new StompException("Malformed message: " + message);
        }

        int beginIndex = 0;
        int endIndex = ((index > 0) && (message.charAt(index - 1) == '\r')) ? index - 1 : index;

        final String messageFrame = message.substring(beginIndex, endIndex);
        final FrameType frame = FrameType.valueOf(messageFrame);

        beginIndex = index + 1;

        Map<String, String> headers = new LinkedHashMap<String, String>();

        boolean hasBody = false;

        while (index >= 0) {
            index = message.indexOf('\n', beginIndex);

            if (index == -1) {
                throw new StompException("Malformed message.");
            }

            endIndex = ((index > 0) && (message.charAt(index - 1) == '\r')) ? index - 1 : index;

            final String header = message.substring(beginIndex, endIndex);

            // Check for termination of headers, the empty line.
            if (header.length() == 0) {
                hasBody = true;
                beginIndex = index + 1;
                break;
            }

            if (header.startsWith(NULL_OCTET)) {
                break;
            }

            int seperatorIndex = header.indexOf(":");

            final String headerKey = header.substring(0, seperatorIndex);
            final String headerValue = header.substring(seperatorIndex + 1);

            headers.put(headerKey, headerValue);
            beginIndex = index + 1;

            if (beginIndex > message.length()) {
                throw new StompException("Malformed message: missing NULL octlet.");
            }

            if (message.charAt(beginIndex) == NULL_OCTET.charAt(0)) {
                break;
            }
        }

        String body = null;

        if (hasBody) {
            endIndex = message.indexOf(NULL_OCTET, beginIndex);
            body = message.substring(beginIndex, endIndex);
        }

        return new StompMessage(frame, headers, body);
    }

    /**
     * Define a STOMP connect message.
     * @param acceptedVersion   the version to accept
     * @param host              the host address
     * @return                  return a STOMP connect message
     */
    public static StompMessage defineConnectMessage(final String acceptedVersion, final String host) {
        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_ACCEPT_VERSION.getValue(), acceptedVersion);
        headers.put(HeaderKey.HEADER_HOST.getValue(), host);

        StompMessage stompMessage = new StompMessage(FrameType.CONNECT, headers, null);

        return stompMessage;
    }

    /**
     * Define a STOMP connected message.
     * @param version           the STOMP version
     * @return                  return a STOMP connected message
     */
    public static StompMessage defineConnectedMessage(final String version) {
        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_VERSION.getValue(), version);

        StompMessage stompMessage = new StompMessage(FrameType.CONNECTED, headers, null);

        return stompMessage;
    }

    /**
     * Define a STOMP disconnect message.
     * @param receipt           the receipt identifier
     * @return                  return a STOMP disconnect message
     */
    public static StompMessage defineDisconnectMessage(final String receipt) {
        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_RECEIPT.getValue(), receipt);

        StompMessage stompMessage = new StompMessage(FrameType.DISCONNECT, headers, null);

        return stompMessage;
    }

    /**
     * Define a STOMP acknowledgement message.
     * @param receiptId         the unique message identifier of the sent message
     * @return                  return a STOMP acknowledgement message
     */
    public static StompMessage defineAckMessage(final String receiptId) {
        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_ID.getValue(), receiptId);

        StompMessage stompMessage = new StompMessage(FrameType.ACK, headers, null);

        return stompMessage;
    }

    /**
     * Define a STOMP subscribe message.
     * @param id                the unique message identifier
     * @param destination       the destination string
     * @param ack               the acknowledgement
     * @return                  return a STOMP subscribe message
     */
    public static StompMessage defineSubscribeMessage(final String id, final String destination, final String ack) {

        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_ID.getValue(), id);
        headers.put(HeaderKey.HEADER_DESTINATION.getValue(), destination);

        if (ack != null) {
            headers.put(HeaderKey.HEADER_ACK.getValue(), ack);
        }

        StompMessage stompMessage = new StompMessage(FrameType.SUBSCRIBE, headers, null);

        return stompMessage;
    }

    /**
     * Define a STOMP un-subscribe message.
     * @param id                the unique message identifier
     * @return                  return a STOMP un-subscribe message
     */
    public static StompMessage defineUnsubscribeMessage(final String id) {
        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_ID.getValue(), id);

        StompMessage stompMessage = new StompMessage(FrameType.UNSUBSCRIBE, headers, null);

        return stompMessage;
    }

    /**
     * Define a STOMP send message with content.
     * @param id                the unique message identifier
     * @param destination       the destination string
     * @param body              the context
     * @return                  return a STOMP send message
     */
    public static StompMessage defineSendMessage(final String id, final String destination, final String body) {
        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_DESTINATION.getValue(), destination);
        if (id != null) {
            headers.put(HeaderKey.HEADER_ID.getValue(), id);
        }

        StompMessage stompMessage = new StompMessage(FrameType.SEND, headers, body);

        return stompMessage;
    }

    /**
     * Define a STOMP error message.
     * @param receiptId         the message ID that caused the exception
     * @param errorHeading      the error heading
     * @param errorDescription  the error description
     * @param relatedRequest    the related request
     * @return                  return a STOMP error message
     */
    public static StompMessage defineErrorMessage(final String receiptId, final String errorHeading, final String errorDescription,
            final String relatedRequest) {

        Map<String, String> headers = new LinkedHashMap<String, String>();

        headers.put(HeaderKey.HEADER_RECEIPT_ID.getValue(), receiptId);
        headers.put(HeaderKey.HEADER_MESSAGE.getValue(), errorHeading);

        String body = null;

        if (relatedRequest != null || errorDescription != null) {
            StringBuilder builder = new StringBuilder();

            if (relatedRequest != null) {
                builder.append("The message:").append(StompMessage.LINE_SPERATOR).append("-----").append(StompMessage.LINE_SPERATOR)
                        .append(relatedRequest).append(StompMessage.LINE_SPERATOR).append("-----").append(StompMessage.LINE_SPERATOR);
            }
            if (errorDescription != null) {
                builder.append(errorDescription).append(StompMessage.LINE_SPERATOR);
            }

            body = builder.toString();
        }

        StompMessage stompMessage = new StompMessage(FrameType.ERROR, headers, body);

        return stompMessage;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((body == null) ? 0 : body.hashCode());
        result = prime * result + ((frame == null) ? 0 : frame.hashCode());
        result = prime * result + ((headers == null) ? 0 : headers.hashCode());
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

        StompMessage other = (StompMessage) obj;

        if (body == null) {
            if (other.body != null) {
                return false;
            }
        } else if (!body.equals(other.body)) {
            return false;
        }

        if (frame != other.frame) {
            return false;
        }

        if (headers == null) {
            if (other.headers != null) {
                return false;
            }
        } else if (!headers.equals(other.headers)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        final String truncatedBody = (body == null || body.length() < 80) ? body : body.substring(0, 80) + "...";
        return "StompMessage [frame=" + frame + ", headers=" + headers + ", body=" + truncatedBody + "]";
    }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(frame);
		out.write(headers.size());
		
		for (String name : headers.keySet()) {
			final String value = headers.get(name);
			
			out.writeObject(name);
			out.writeObject(value);
		}
		
		out.writeObject(body);		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    frame = (FrameType) in.readObject();
	    headers = new HashMap<String, String>();
	    
        final int count = in.readInt();

	    for (int i = 0; i < count; i++) {
	    	final String name = (String) in.readObject();
	    	final String value = (String) in.readObject();
	    	
	    	headers.put(name,  value);
	    }
	    
	    body = (String) in.readObject();		
	}
}
