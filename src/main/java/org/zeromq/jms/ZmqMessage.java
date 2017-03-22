package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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
public class ZmqMessage implements Message, Externalizable {

    private final Map<String, Object> properties = new HashMap<String, Object>();

    private String correlationID;
    private int deliveryMode;
    private Destination destrination;
    private long expiration;
    private String messageID;
    private int priority;
    private boolean redelivered;
    private Destination replyTo;
    private long timestamp;
    private String type;
    private long deliveryTime;
        
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
        return (Boolean) properties.get(name);
    }

    @Override
    public byte getByteProperty(final String name) throws JMSException {
        return (Byte) properties.get(name);
    }

    @Override
    public double getDoubleProperty(final String name) throws JMSException {
        return (Double) properties.get(name);
    }

    @Override
    public float getFloatProperty(final String name) throws JMSException {
        return (Float) properties.get(name);
    }

    @Override
    public int getIntProperty(final String name) throws JMSException {
        return (Integer) properties.get(name);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return correlationID;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        if (correlationID == null) {
            return null;
        }

        return correlationID.getBytes();
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
        return (Long) properties.get(name);
    }

    @Override
    public Object getObjectProperty(final String name) throws JMSException {
        return properties.get(name);
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        final Vector<String> names = new Vector<String>(properties.keySet());

        return names.elements();
    }

    @Override
    public short getShortProperty(final String name) throws JMSException {
        return (Short) properties.get(name);
    }

    @Override
    public String getStringProperty(final String name) throws JMSException {
        return (String) properties.get(name);
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
        this.correlationID = new String(correlationIDAsBytes);
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
	public long getJMSDeliveryTime() throws JMSException {
        return deliveryTime;
	}

	@Override
	public void setJMSDeliveryTime(final long deliveryTime) throws JMSException {
        this.deliveryTime = deliveryTime;
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
	public <T> T getBody(Class<T> c) throws JMSException {
		// Message (but not one of its subtypes) then this parameter may be set
		// to any type; the returned value will always be null.
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean isBodyAssignableTo(final Class c) throws JMSException {

        throw new UnsupportedOperationException();
	}

	@Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((messageID == null) ? 0 : messageID.hashCode());
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

        ZmqMessage other = (ZmqMessage) obj;

        if (messageID == null) {
            if (other.messageID != null) {
                return false;
            }
        } else if (!messageID.equals(other.messageID)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ZmqMessage [properties=" + properties + ", correlationID=" + correlationID + ", deliveryMode=" + deliveryMode + ", destrination="
                + destrination + ", expiration=" + expiration + ", messageID=" + messageID + ", priority=" + priority + ", redelivered="
                + redelivered + ", timestamp=" + timestamp + ", type=" + type + ", deliveryTime=" + deliveryTime + "]";
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeObject(correlationID);
        out.writeInt(deliveryMode);
        out.writeObject(destrination);
        out.writeLong(expiration);
        out.writeObject(messageID);
        out.writeInt(priority);
        out.writeBoolean(redelivered);
        out.writeObject(replyTo);
        out.writeLong(timestamp);
        out.writeObject(type);
        out.writeLong(deliveryTime);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        correlationID = (String) in.readObject();
        deliveryMode = in.readInt();
        destrination = (Destination) in.readObject();
        expiration = in.readLong();
        messageID = (String) in.readObject();
        priority = in.readInt();
        redelivered = in.readBoolean();
        replyTo = (Destination) in.readObject();
        timestamp = in.readLong();
        type = (String) in.readObject();
        deliveryTime = in.readLong();
    }
}
