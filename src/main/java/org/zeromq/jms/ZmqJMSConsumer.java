package org.zeromq.jms;

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

public class ZmqJMSConsumer implements JMSConsumer {

	@SuppressWarnings("unused")
	private final ZmqJMSContext context;
	private final MessageConsumer consumer;
	
	public ZmqJMSConsumer(final ZmqJMSContext context, MessageConsumer consumer) {
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
	public Message receive(long timeout) {
        try {
    		final Message message = consumer.receive(timeout);
    		
    		return message;
	    } catch (JMSException ex) {
	        throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
	    }
	}

	@Override
	public <T> T receiveBody(Class<T> c) {
	    try {
	        Message message = consumer.receive();
	        //context.setLastMessage(ActiveMQJMSConsumer.this, message);
	        return message == null ? null : message.getBody(c);
	    } catch (JMSException ex) {
	        throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
	}

	@Override
	public <T> T receiveBody(Class<T> c, long timeout) {
	    try {
	        Message message = consumer.receive(timeout);
	        //context.setLastMessage(ActiveMQJMSConsumer.this, message);
	        return message == null ? null : message.getBody(c);
	    } catch (JMSException ex) {
	        throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
	}

	@Override
	public <T> T receiveBodyNoWait(Class<T> c) {
	    try {
	        Message message = consumer.receiveNoWait();
	        //context.setLastMessage(ActiveMQJMSConsumer.this, message);
	        return message == null ? null : message.getBody(c);
	    } catch (JMSException ex) {
	        throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
        }
	}

	@Override
	public Message receiveNoWait() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        try {
            consumer.setMessageListener(listener);
	    } catch (JMSException ex) {
	        throw new JMSRuntimeException(ex.getMessage(), ex.getErrorCode(), ex);
	    }
	}

}
