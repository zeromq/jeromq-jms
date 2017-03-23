package org.zeromq.jms;

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * ZMQ implementation of the JMSConsumer interface.
 */
public class ZmqJMSConsumer implements JMSConsumer {

    @SuppressWarnings("unused")
    private final ZmqJMSContext context;
    private final MessageConsumer consumer;

    /**
     * Construct the ZMQ JMS Consumer.
     * @param context   the context
     * @param consumer  the inner message consumer interface
     */
    public ZmqJMSConsumer(final ZmqJMSContext context, final MessageConsumer consumer) {
        this.context = context;
        this.consumer = consumer;
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        try {
            return consumer.getMessageListener();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public String getMessageSelector() {
        try {
            return consumer.getMessageSelector();
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Message receive() {
        try {
            final Message message = consumer.receive();

            return message;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Message receive(final long timeout) {
        try {
            final Message message = consumer.receive(timeout);

            return message;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public <T> T receiveBody(final Class<T> c) {
        try {
            Message message = consumer.receive();

            return message == null ? null : message.getBody(c);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public <T> T receiveBody(final Class<T> c, final long timeout) {
        try {
            Message message = consumer.receive(timeout);
            //context.setLastMessage(ActiveMQJMSConsumer.this, message);
            return message == null ? null : message.getBody(c);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(final Class<T> c) {
        try {
            Message message = consumer.receiveNoWait();

            return message == null ? null : message.getBody(c);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            Message message = consumer.receiveNoWait();

            return message;
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

    @Override
    public void setMessageListener(final MessageListener listener) throws JMSRuntimeException {
        try {
            consumer.setMessageListener(listener);
        } catch (JMSException ex) {
            throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
    }

}
