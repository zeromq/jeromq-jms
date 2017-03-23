package org.zeromq.jms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

/**
 * Zero MQ implementation of a JMS Byte Message.
 */
public class ZmqByteMessage extends ZmqMessage implements BytesMessage {

    private byte[] content = null;

    private ByteArrayInputStream inByteArray = null;
    private ByteArrayOutputStream outByteArray = null;

    private DataInputStream inData = null;
    private DataOutputStream outData = null;

    /**
     * Construct ZMQ Byte Message.
     */
    public ZmqByteMessage() {
        super();

        // Set to "Write" mode
        outByteArray = new ByteArrayOutputStream();
        outData = new DataOutputStream(outByteArray);
    }

    @Override
    public void clearBody() throws JMSException {
        if (inData != null) {
            try {
                inData.close();
            } catch (IOException ex) {
                throw new ZmqException("Unable to reset the message mode.", ex);
            }

            inByteArray = null;
            inData = null;
        }

        if (outData != null) {
            try {
                outData.close();
            } catch (IOException ex) {
                throw new ZmqException("Unable to reset the message mode.", ex);
            }
        }

        // Set to "Write" mode
        outByteArray = new ByteArrayOutputStream();
        outData = new DataOutputStream(outByteArray);

        content = null;
    }

    @Override
    public void reset() throws JMSException {
        if (outData != null) {
            content = outByteArray.toByteArray();

            try {
                outData.close();
            } catch (IOException ex) {
                throw new ZmqException("Unable to reset the message mode.", ex);
            }

            outByteArray = null;
            outData = null;
        }

        if (inData != null) {
            try {
                inData.close();
            } catch (IOException ex) {
                throw new ZmqException("Unable to reset the message mode.", ex);
            }
        }

        if (content == null) {
            throw new ZmqException("Unable to reset the message mode, due to missing content.");
        }

        // Set to "Write" mode
        inByteArray = new ByteArrayInputStream(content);
        inData = new DataInputStream(inByteArray);
    }

    /**
     * Check for read mode.
     * @throws MessageNotReadableException  throws exception when not in correct state
     */
    private void checkReadMode() throws MessageNotReadableException {
        if (outData == null) {
            throw new MessageNotReadableException("Message no in read mode.");
        }

        if (inData != null) {
            throw new MessageNotReadableException("Message no in read mode.");
        }
    }

    /**
     * Check for write mode.
     * @throws MessageNotWriteableException   throws exception when not in correct state
     */
    private void checkWriteMode() throws MessageNotWriteableException {
        if (inData == null) {
            throw new MessageNotWriteableException("Message no in read mode.");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getBody(final Class<T> c) throws JMSException {
        return (T) content;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean isBodyAssignableTo(final Class c) throws JMSException {
        return c.isAssignableFrom(Byte[].class);
    }

    @Override
    public long getBodyLength() throws JMSException {
        return (content == null) ? 0 : content.length;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        checkReadMode();
        try {
            return inData.readBoolean();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        checkReadMode();
        try {
            return inData.readByte();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public int readBytes(final byte[] value) throws JMSException {
        checkReadMode();
        try {
            return inData.read(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public int readBytes(final byte[] value, final int length) throws JMSException {
        checkReadMode();
        try {
            return inData.read(value, 0, length);
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public char readChar() throws JMSException {
        checkReadMode();
        try {
            return inData.readChar();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        checkReadMode();
        try {
            return inData.readDouble();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        checkReadMode();
        try {
            return inData.readFloat();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public int readInt() throws JMSException {
        checkReadMode();
        try {
            return inData.readInt();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public long readLong() throws JMSException {
        checkReadMode();
        try {
            return inData.readLong();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public short readShort() throws JMSException {
        checkReadMode();
        try {
            return inData.readShort();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        checkReadMode();
        try {
            return inData.readUTF();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        checkReadMode();
        try {
            return inData.readUnsignedByte();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        checkReadMode();
        try {
            return inData.readUnsignedShort();
        } catch (IOException ex) {
            throw new ZmqException("Unable to read from stream.", ex);
        }
    }


    @Override
    public void writeBoolean(final boolean value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeBoolean(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeByte(final byte value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeByte(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeBytes(final byte[] value) throws JMSException {
        checkWriteMode();
        try {
            outData.write(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException {
        checkWriteMode();
        try {
            outData.write(value, offset, length);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeChar(final char value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeChar(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeDouble(final double value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeDouble(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeFloat(final float value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeFloat(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeInt(final int value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeInt(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeLong(final long value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeLong(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeObject(final Object value) throws JMSException {
        checkWriteMode();

        if (value == null) {
            throw new NullPointerException();
        }

        if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Character) {
            writeChar(((Character) value).charValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Long) {
            writeLong(((Long) value).longValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass());
        }
    }

    @Override
    public void writeShort(final short value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeShort(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }

    @Override
    public void writeUTF(final String value) throws JMSException {
        checkWriteMode();
        try {
            outData.writeUTF(value);
        } catch (IOException ex) {
            throw new ZmqException("Unable to write into message.", ex);
        }
    }
}
