package org.zeromq.jms.jndi;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.naming.ConfigurationException;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

import org.zeromq.jms.ZmqConnectionFactory;
import org.zeromq.jms.ZmqQueue;
import org.zeromq.jms.ZmqTopic;

/*
 <Context ...>
 ...
 <Resource name="bean/MyBeanFactory" auth="Container"
 type="com.mycompany.MyBean"
 factory="com.mycompany.MyBeanFactory"
 bar="23"/>
 ...

 <Resource name="jms/ConnectionFactory" auth="Container"
 type="org.apache.activemq.ActiveMQConnectionFactory"
 description="JMS Connection Factory"
 factory="org.apache.activemq.jndi.JNDIReferenceFactory"
 brokerURL="tcp://localhost:61616"
 brokerName="LocalActiveMQBroker"
 userName="admin" password="admin"
 useEmbeddedBroker="false"
 clientID="TomcatClientID" />

 <Resource name="jms/myQueue" auth="Container"
 type="org.apache.activemq.command.ActiveMQQueue"
 description="JMS Queue"
 factory="org.apache.activemq.jndi.JNDIReferenceFactory"
 physicalName="app.jms.queue" />

 <Resource name="jms/ConnectionFactory" auth="Container"
 type="net.sandbox.jms.ZmqConnectionFactory"
 description="0MZ JMS Connection Factory"
 factory="net.sandbox.jms.JNDIReferenceFactory"
 destinationURL.1="jms:queue:queue_1?zmq.addr=tcp://*:9705&amp;jms.retry=3"
 destinationURL.2="jms:queue:queue_2?zmq.addr=tcp://*:9706&amp;jms.retry=0&amp;jms.selector=(someProperty=�abcd�)"
 destinationURL.3="jms:topic:topic_1?zmq.addr=tcp://*:9707"/>

 <Resource name="jms/ConnectionFactory" auth="Container"
 type="org.zeromq.jms.ZmqConnectionFactory"
 description="0MZ JMS Connection Factory"
 factory="org.zeromq.jms.protocol.ZmqObjectFactory"
 gatewayFactory="net.org.zeromq.jms.protocol.ZmqGatewayFactory"
 gatewayExtension.1="org.plugin.zmq"
 destinationURL.1="jms:queue:queue_1?zmq.addr=tcp://*:9705&amp;jms.retry=3"
 destinationURL.2="jms:queue:queue_2?zmq.addr=tcp://*:9706&amp;jms.retry=0&amp;jms.selector=(someProperty=�abcd�)"
 destinationURL.3="jms:topic:topic_1?zmq.addr=tcp://*:9707"/>

 <Resource name="jms/myQueue" auth="Container"
 type="org.zeromq.jms.ZmqQueue"
 description="0MZ JMS Queue"
 factory="net.sandbox.jms.JNDIReferenceFactory"
 physicalName="queue_1"/>

 </Context>
 */
/**
 * Zero MQ JNI object factory implementation to enable JNDI lookups of Zero MQ factories and
 * destinations (i.e. Queues and Topics).
 */
public class ZmqObjectFactory implements ObjectFactory {

    private static final Logger LOGGER = Logger.getLogger(ZmqObjectFactory.class.getCanonicalName());

    /**
     * Return the property value within the specified collection of properties.
     * @param properties  the collection of properties
     * @param name        the name of the property to find
     * @return            return the property value
     */
    private String getPropertyValue(final Properties properties, final String name) {
        return (String) properties.getProperty(name);
    }

    /**
     * Return the property values within the specified collection of properties.
     * @param properties  the collection of properties
     * @param nameRegex   the regular expression to check property names against
     * @return            return the property values
     */

    private String[] getPropertyValues(final Properties properties, final String nameRegex) {
        final Pattern pattern = Pattern.compile(nameRegex, Pattern.CASE_INSENSITIVE);

        final Enumeration<?> propertyNameEnum = properties.propertyNames();
        final List<String> valueList = new ArrayList<String>();

        while (propertyNameEnum.hasMoreElements()) {
            final String name = (String) propertyNameEnum.nextElement();

            if (!pattern.matcher(name).matches()) {
                final String value = (String) properties.getProperty(name);

                if (value != null) {
                    valueList.add(value);
                }
            }
        }

        final String[] values = new String[valueList.size()];
        valueList.toArray(values);

        return values;
    }

    @Override
    /**
     * This will be called by a JNDIprovider when a Reference is retrieved from a JNDI store - and generates the original instance
     * object       the Reference object name
     * the          JNDI name
     * nameCtx      the context
     * environment  the environment settings used by JNDI
     */
    public Object getObjectInstance(final Object object, final Name name, final Context nameCtx, final Hashtable<?, ?> environment) throws Exception {

        if (object instanceof Reference) {
            final Reference reference = (Reference) object;
            final String className = reference.getClassName();

            LOGGER.info("Getting instance of " + className);

            Properties properties = new Properties();

            for (Enumeration<RefAddr> iter = reference.getAll(); iter.hasMoreElements();) {
                final StringRefAddr addr = (StringRefAddr) iter.nextElement();

                properties.put(addr.getType(), (addr.getContent() == null) ? "" : addr.getContent());
            }

            if (className.equals(ZmqConnectionFactory.class.getName())) {
                final String gatewayFactory = properties.getProperty("gatewayFactory", null);
                final String[] gatewayExtensions = getPropertyValues(properties, "^gatewayExtension//..*");
                final String[] destinationValues = getPropertyValues(properties, "^destinationURL//..*");

                return new ZmqConnectionFactory(gatewayFactory, destinationValues, gatewayExtensions);
            } else if (className.equals(ZmqQueue.class.getName())) {
                final String physicalName = getPropertyValue(properties, "physicalName");

                if (physicalName == null) {
                    throw new ConfigurationException("Missing attribute 'physicalName' to build class: " + className);
                }

                return new ZmqQueue(physicalName);
            } else if (className.equals(ZmqTopic.class.getName())) {
                final String physicalName = getPropertyValue(properties, "physicalName");

                if (physicalName == null) {
                    throw new ConfigurationException("Missing attribute 'physicalName' to build class: " + className);
                }

                return new ZmqTopic(physicalName);
            } else {
                throw new ConfigurationException("Unable to instanciate class: " + className);
            }
        } else {
            LOGGER.severe("Object " + object + " is not a reference - cannot load");

            throw new RuntimeException("Object " + object + " is not a reference");
        }
    }
}
