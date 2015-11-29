package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 *  Zero MQ JMS message implementation, which is a concrete class and is the root of  all ZMQ JMS messages.
 */
public class ZmqMessage implements Message, Serializable {

    private static final long serialVersionUID = 6299240265328072315L;

    private final Map<String, Object> properties = new HashMap<String, Object>();

    private String correlationID;
    private byte[] correlationIDAsBytes;
    private int deliveryMode;
    private Destination destrination;
    private long expiration;
    private String messageID;
    private int priority;
    private boolean redelivered;
    private Destination replyTo;
    private long timestamp;
    private String type;

    /**
     * Setter to return the properties. This is a back door for the library to get the property bag for
     * selector variable resolution
     * @return  return the internal property bag.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public void acknowledge() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void clearBody() throws JMSException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void clearProperties() throws JMSException {

        properties.clear();
    }

    @Override
    public boolean getBooleanProperty(final String name) throws JMSException {

        final Boolean value = (Boolean) properties.get(name);
        return value;
    }

    @Override
    public byte getByteProperty(final String name) throws JMSException {

        final Byte value = (Byte) properties.get(name);
        return value;
    }

    @Override
    public double getDoubleProperty(final String name) throws JMSException {

        final Double value = (Double) properties.get(name);
        return value;
    }

    @Override
    public float getFloatProperty(final String name) throws JMSException {

        final Float value = (Float) properties.get(name);
        return value;
    }

    @Override
    public int getIntProperty(final String name) throws JMSException {

        final Integer value = (Integer) properties.get(name);
        return value;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {

        return correlationID;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {

        return correlationIDAsBytes;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {

        return deliveryMode;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {

        return destrination;
    }

    @Override
    public long getJMSExpiration() throws JMSException {

        return expiration;
    }

    @Override
    public String getJMSMessageID() throws JMSException {

        return messageID;
    }

    @Override
    public int getJMSPriority() throws JMSException {

        return priority;
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {

        return redelivered;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {

        return replyTo;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {

        return timestamp;
    }

    @Override
    public String getJMSType() throws JMSException {

        return type;
    }

    @Override
    public long getLongProperty(final String name) throws JMSException {

        final Long value = (Long) properties.get(name);
        return value;
    }

    @Override
    public Object getObjectProperty(final String name) throws JMSException {

        final Object value = properties.get(name);
        return value;
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {

        final Vector<String> names = new Vector<String>(properties.keySet());
        return names.elements();
    }

    @Override
    public short getShortProperty(final String name) throws JMSException {

        final Short value = (Short) properties.get(name);
        return value;
    }

    @Override
    public String getStringProperty(final String name) throws JMSException {

        final String value = (String) properties.get(name);
        return value;
    }

    @Override
    public boolean propertyExists(final String name) throws JMSException {
        return properties.containsKey(name);
    }

    @Override
    public void setBooleanProperty(final String name, final boolean value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setByteProperty(final String name, final byte value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setDoubleProperty(final String name, final double value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setFloatProperty(final String name, final float value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setIntProperty(final String name, final int value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setJMSCorrelationID(final String correlationID) throws JMSException {

        this.correlationID = correlationID;
    }

    @Override
    public void setJMSCorrelationIDAsBytes(final byte[] correlationIDAsBytes) throws JMSException {

        this.correlationIDAsBytes = correlationIDAsBytes;
    }

    @Override
    public void setJMSDeliveryMode(final int deliveryMode) throws JMSException {

        this.deliveryMode = deliveryMode;
    }

    @Override
    public void setJMSDestination(final Destination destrination) throws JMSException {

        this.destrination = destrination;
    }

    @Override
    public void setJMSExpiration(final long expiration) throws JMSException {

        this.expiration = expiration;
    }

    @Override
    public void setJMSMessageID(final String messageID) throws JMSException {

        this.messageID = messageID;
    }

    @Override
    public void setJMSPriority(final int priority) throws JMSException {

        this.priority = priority;
    }

    @Override
    public void setJMSRedelivered(final boolean redelivered) throws JMSException {

        this.redelivered = redelivered;
    }

    @Override
    public void setJMSReplyTo(final Destination replyTo) throws JMSException {

        this.replyTo = replyTo;
    }

    @Override
    public void setJMSTimestamp(final long timestamp) throws JMSException {

        this.timestamp = timestamp;
    }

    @Override
    public void setJMSType(final String type) throws JMSException {

        this.type = type;
    }

    @Override
    public void setLongProperty(final String name, final long value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setObjectProperty(final String name, final Object value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setShortProperty(final String name, final short value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public void setStringProperty(final String name, final String value) throws JMSException {

        properties.put(name, value);
    }

    @Override
    public String toString() {
        return "ZmqMessage [properties=" + properties + ", correlationID=" + correlationID + ", deliveryMode=" + deliveryMode + ", destrination="
                + destrination + ", expiration=" + expiration + ", messageID=" + messageID + ", priority=" + priority + ", redelivered="
                + redelivered + ", timestamp=" + timestamp + ", type=" + type + "]";
    }
}
