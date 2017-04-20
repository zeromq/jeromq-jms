package org.zeromq.jms.json;

import java.util.Date;

/**
 *  JSON message.
 */
public class TestJsonMessage {
    /**
     * Message type enumeration.
     */
    enum MessageType { HEARTBREAT, SENT, ACK };

    private String messageId;
    private MessageType messageType;
    private Date sentDate;
    private String body;

    /**
     * @return  return message identifier
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Set the unique message identifier.
     * @param messageId  the message identifier
     */
    public void setMessageId(final String messageId) {
        this.messageId = messageId;
    }

    /**
     * @return  return the message type
     */
    public MessageType getMessageType() {
        return messageType;
    }

    /**
     * Set the for the message type.
     * @param messageType  the type of message, i.e. ACK, HEARTBEAT, etc...
     */
    public void setMessageType(final MessageType messageType) {
        this.messageType = messageType;
    }

    /**
     * @return  return the sent data
     */
    public Date getSentDate() {
        return sentDate;
    }

    /**
     * Set the sent date.
     * @param sentDate  the sent date
     */
    public void setSentDate(final Date sentDate) {
        this.sentDate = sentDate;
    }

    /**
     * @return  return the body of the message
     */
    public String getBody() {
        return body;
    }

    /**
     * Set the body of the message.
     * @param body  the body
     */
    public void setBody(final String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "JsonMessage [messageId=" + messageId + ", messageType=" + messageType + ", sentDate=" + sentDate + ", body=" + body + "]";
    }
}
