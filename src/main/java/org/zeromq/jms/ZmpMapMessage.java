package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.jms.JMSException;
import javax.jms.MapMessage;

/**
 * Zero MQ implementation of a JMS Map Message.
 */
public class ZmpMapMessage extends ZmqMessage implements MapMessage {

    private static final long serialVersionUID = 4463177134955671773L;

    private Map<String, Object> map = new HashMap<String, Object>();

    @Override
    public boolean getBoolean(final String name) throws JMSException {
        return (Boolean) map.get(name);
    }

    @Override
    public byte getByte(final String name) throws JMSException {
        return (Byte) map.get(name);
    }

    @Override
    public byte[] getBytes(final String name) throws JMSException {
        return (byte[]) map.get(name);
    }

    @Override
    public char getChar(final String name) throws JMSException {
        return (Character) map.get(name);
    }

    @Override
    public double getDouble(final String name) throws JMSException {
        return (Double) map.get(name);
    }

    @Override
    public float getFloat(final String name) throws JMSException {
        return (Float) map.get(name);
    }

    @Override
    public int getInt(final String name) throws JMSException {
        return (Integer) map.get(name);
    }

    @Override
    public long getLong(final String name) throws JMSException {
        return (Long) map.get(name);
    }

    @Override
    public Enumeration<String> getMapNames() throws JMSException {
        Vector<String> names = new Vector<String>(map.keySet());

        return names.elements();
    }

    @Override
    public Object getObject(final String name) throws JMSException {
        return map.get(name);
    }

    @Override
    public short getShort(final String name) throws JMSException {
        return (Short) map.get(name);
    }

    @Override
    public String getString(final String name) throws JMSException {
        return (String) map.get(name);
    }

    @Override
    public boolean itemExists(final String name) throws JMSException {
        return map.containsKey(name);
    }

    @Override
    public void setBoolean(final String name, final boolean value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setByte(final String name, final byte value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setBytes(final String name, final byte[] values) throws JMSException {
        map.put(name, values);
    }

    @Override
    public void setBytes(final String name, final byte[] values, final int startPos, final int endPos) throws JMSException {
        final int size = endPos - startPos + 1;
        final byte[] subValues = new byte[size];

        System.arraycopy(values, startPos, subValues, 0, size);

        map.put(name, subValues);
    }

    @Override
    public void setChar(final String name, final char value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setDouble(final String name, final double value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setFloat(final String name, final float value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setInt(final String name, final int value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setLong(final String name, final long value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setObject(final String name, final Object value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setShort(final String name, final short value) throws JMSException {
        map.put(name, value);
    }

    @Override
    public void setString(final String name, final String value) throws JMSException {
        map.put(name, value);
    }
}
