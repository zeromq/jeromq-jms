package org.zeromq.jms;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * ZMQ implementation of the JMSProcess interface.
 */
public class ZmqJMSProducer implements JMSProducer {

    private final ZmqJMSContext context;
    private final MessageProducer messageProducer;
    private final Map<String, Object> properties = new HashMap<String, Object>();

    private String jmsType;
    private String jmsCorrelationID;
    private Destination jmsReplyTo;

    private volatile CompletionListener completionListener;

    /**
     * Construct the ZMQ producer class.
     * @param context          the context
     * @param messageProducer  the inner producer message instance
     */
    public ZmqJMSProducer(final ZmqJMSContext context, final MessageProducer messageProducer) {
        this.context = context;
        this.messageProducer = messageProducer;
    }

    /**
     * Check the validity of the property name, throwing JMSRuntimeException when it
     * is not valid.
     * @param name  the property name
     */
    private void checkPropertyName(final String name) {
        if (name == null || name.length() == 0) {
            throw new JMSRuntimeException("Invalid property name: " + name);
        }
    }

    @Override
    public JMSProducer clearProperties() {
        properties.clear();

        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    @Override
    public boolean getBooleanProperty(final String name) {
        checkPropertyName(name);

        return (Boolean) properties.get(name);
    }

    @Override
    public byte getByteProperty(final String name) {
        checkPropertyName(name);

        return (Byte) properties.get(name);
    }

    @Override
    public long getDeliveryDelay() {
        try {
            return messageProducer.getDeliveryDelay();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public int getDeliveryMode() {
        try {
            return messageProducer.getDeliveryMode();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public boolean getDisableMessageID() {
        try {
            return messageProducer.getDisableMessageID();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        try {
            return messageProducer.getDisableMessageTimestamp();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public double getDoubleProperty(final String name) {
        checkPropertyName(name);

        return (Double) properties.get(name);
    }

    @Override
    public float getFloatProperty(final String name) {
        checkPropertyName(name);

        return (Float) properties.get(name);
    }

    @Override
    public int getIntProperty(final String name) {
        checkPropertyName(name);

        return (Integer) properties.get(name);
    }

    @Override
    public String getJMSCorrelationID() {
        return jmsCorrelationID;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return jmsCorrelationID.getBytes();
    }

    @Override
    public Destination getJMSReplyTo() {
        return jmsReplyTo;
    }

    @Override
    public String getJMSType() {
        return jmsType;
    }

    @Override
    public long getLongProperty(final String name) {
        checkPropertyName(name);

        return (Long) properties.get(name);
    }

    @Override
    public Object getObjectProperty(final String name) {
        checkPropertyName(name);

        return properties.get(name);
    }

    @Override
    public int getPriority() {
        try {
            return messageProducer.getPriority();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public short getShortProperty(final String name) {
        checkPropertyName(name);

        return (Short) properties.get(name);
    }

    @Override
    public String getStringProperty(final String name) {
        checkPropertyName(name);

        return (String) properties.get(name);
    }

    @Override
    public long getTimeToLive() {
        try {
            return messageProducer.getTimeToLive();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public boolean propertyExists(final String name) {
        checkPropertyName(name);

        return properties.containsKey(name);
    }

    @Override
    public JMSProducer send(final Destination destination, final Message message) {
        if (message == null) {
            throw new MessageFormatRuntimeException("Cannot send null messages.");
        }

        try {
            if (jmsCorrelationID != null) {
                message.setJMSCorrelationID(jmsCorrelationID);
            }

            if (jmsCorrelationID != null && jmsCorrelationID.length() > 0) {
                message.setJMSCorrelationID(jmsCorrelationID);
            }

            if (jmsReplyTo != null) {
                message.setJMSReplyTo(jmsReplyTo);
            }

            if (jmsType != null) {
                message.setJMSType(jmsType);
            }

            // Set all the properties
            for (String name : properties.keySet()) {
                message.setObjectProperty(name, properties.get(name));
            }

            if (completionListener != null) {
                messageProducer.send(destination, message, completionListener);
            } else {
                messageProducer.send(destination, message);
            }
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
        return this;
    }

    @Override
    public JMSProducer send(final Destination destination, final String body) {
        final TextMessage message = context.createTextMessage(body);
        send(destination, message);

        return this;
    }

    @Override
    public JMSProducer send(final Destination destination, final Map<String, Object> body) {
        final MapMessage message = context.createMapMessage();

        try {
               if (body != null) {
                // Set all the properties
                for (String name : body.keySet()) {
                    message.setObjectProperty(name, body.get(name));
                }
            }

            send(destination, message);

            return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer send(final Destination destination, final byte[] body) {
        final BytesMessage message = context.createBytesMessage();

        try {
            if (body != null) {
                message.writeBytes(body);
            }

            send(destination, message);

            return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer send(final Destination destination, final Serializable body) {
        final ObjectMessage message = context.createObjectMessage();

        try {
            if (body != null) {
                message.setObject(body);
            }

            send(destination, message);

            return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer setAsync(final CompletionListener completionListener) {
        synchronized (this) {
            this.completionListener = completionListener;
        }

        return this;
    }

    @Override
    public JMSProducer setDeliveryDelay(final long deliveryDelay) {
        try {
           messageProducer.setDeliveryDelay(deliveryDelay);

           return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer setDeliveryMode(final int deliveryMode) {
        try {
           messageProducer.setDeliveryMode(deliveryMode);

           return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer setDisableMessageID(final boolean value) {
        try {
           messageProducer.setDisableMessageID(value);

           return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(final boolean value) {
        try {
               messageProducer.setDisableMessageTimestamp(value);

               return this;
            } catch (JMSException ex) {
                throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
            }
    }

    @Override
    public JMSProducer setJMSCorrelationID(final String jmsCorrectionId) {
        this.jmsCorrelationID = jmsCorrectionId;

        return this;
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(final byte[] jmsCorrectionId) {
        this.jmsCorrelationID = new String(jmsCorrectionId);

        return this;
    }

    @Override
    public JMSProducer setJMSReplyTo(final Destination jmsReplyTo) {
        this.jmsReplyTo = jmsReplyTo;

        return this;
    }

    @Override
    public JMSProducer setJMSType(final String jmsType) {
        this.jmsType = jmsType;

        return this;
    }

    @Override
    public JMSProducer setPriority(final int defaultPriority) {
        try {
            messageProducer.setPriority(defaultPriority);

            return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public JMSProducer setProperty(final String name, final boolean value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final byte value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final short value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final int value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final long value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final float value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final double value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final String value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setProperty(final String name, final Object value) {
        properties.put(name,  value);

        return this;
    }

    @Override
    public JMSProducer setTimeToLive(final long timeToLive) {
        try {
            messageProducer.setTimeToLive(timeToLive);

            return this;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }
}
