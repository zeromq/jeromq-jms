package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.zeromq.jms.protocol.ZmqGatewayFactory;

/**
 * Generic Zero MQ JMS connection factory for queues and topics.
 */
public class ZmqConnectionFactory implements QueueConnectionFactory, TopicConnectionFactory {

    private static final Logger LOGGER = Logger.getLogger(ZmqConnectionFactory.class.getCanonicalName());

    private final Map<String, ZmqURI> destinationSchema = new HashMap<String, ZmqURI>();
    private String gatewayFactoryClassName = ZmqGatewayFactory.class.getCanonicalName();
    private String[] packageNameExtensions = null;

    /**
     * Construct Zero MQ connection factory.
     */
    public ZmqConnectionFactory() {
    }

    /**
     * Construct Zero MQ connection factory with the specified destination URIs.
     * @param destinations  the destination URIs
     */
    public ZmqConnectionFactory(final String[] destinations) {
        if (destinations != null && destinations.length > 0) {
            setDestinations(destinations);
        }
    }

    /**
     * Construct Zero MQ connection factory with the specified destination URIs.
     * @param gatewayFactoryClassName  the class name of the gateway factory
     * @param packageNameExtensions    the array of packages containing extension to ZERO MQ
     * @param destinations             the destination URIs
     */
    public ZmqConnectionFactory(final String gatewayFactoryClassName, final String[] packageNameExtensions, final String[] destinations) {
        this.gatewayFactoryClassName = gatewayFactoryClassName;
        this.packageNameExtensions = packageNameExtensions;

        if (destinations != null && destinations.length > 0) {
            setDestinations(destinations);
        }
    }

    /**
     * Setter for the list of destinations.
     * @param destinations  the URI lost of destinations
     */
    public void setDestinations(final String[] destinations) {
        for (int i = 0; i < destinations.length; i++) {
            final ZmqURI uri = ZmqURI.create(destinations[i]);
            final String uriDesintationName = uri.getDestinationName();

            if (destinationSchema.containsKey(uriDesintationName)) {
                throw new JMSRuntimeException("Existing URI within the schema have been allocated the destrinaiton name: " + uri);
            }

            destinationSchema.put(uriDesintationName, uri);
        }
    }

    @Override
    public Connection createConnection() throws JMSException {
        LOGGER.info("Create connection");

        return createQueueConnection();
    }

    @Override
    public Connection createConnection(final String userName, final String password) throws JMSException {
        LOGGER.info("Create connection");

        return createQueueConnection(userName, password);
    }

    /**
     * @return               return a factory gateway
     * @throws ZmqException  throws exception when factory construction fails
     */
    private ZmqGatewayFactory getFactoryGateway() throws ZmqException {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends ZmqGatewayFactory> gatewayFactoryClass = (Class<? extends ZmqGatewayFactory>) Class
                    .forName(gatewayFactoryClassName);

            final Constructor<?> gatewayFactoryConstructor = gatewayFactoryClass.getConstructor(String[].class, Map.class);

            final ZmqGatewayFactory gatewayFactory = (ZmqGatewayFactory) gatewayFactoryConstructor.newInstance(packageNameExtensions,
                    destinationSchema);

            return gatewayFactory;
        } catch (ClassNotFoundException ex) {
            throw new ZmqException("Class could not be found.", ex);
        } catch (NoSuchMethodException | SecurityException ex) {
            throw new ZmqException("Unable to find required constructor for class: " + gatewayFactoryClassName, ex);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new ZmqException("Unable to invoke constructor for class: " + gatewayFactoryClassName, ex);
        }

    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createQueueConnection(null, null);
    }

    @Override
    public QueueConnection createQueueConnection(final String userName, final String password) throws JMSException {
        LOGGER.info("Create queue connection");

        ZmqGatewayFactory gatewayFactory = getFactoryGateway();
        QueueConnection connection = new ZmqConnection(gatewayFactory, destinationSchema);

        return connection;
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createTopicConnection(null, null);
    }

    @Override
    public TopicConnection createTopicConnection(final String userName, final String password) throws JMSException {
        LOGGER.info("Create topic connection");

        ZmqGatewayFactory gatewayFactory = getFactoryGateway();
        TopicConnection connection = new ZmqConnection(gatewayFactory, destinationSchema);

        return connection;
    }

    @Override
    public JMSContext createContext() {

       return createContext(null, null, ZmqSession.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(final int sessionMode) {

           return createContext(null, null, sessionMode);
    }

    @Override
    public JMSContext createContext(final String userName, final String password) {

           return createContext(userName, password, ZmqSession.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(final String userName, final String password, final int sessionMode) {

        LOGGER.info("Create queue connection");

        try {
            ZmqGatewayFactory gatewayFactory = getFactoryGateway();
            ZmqConnection connection = new ZmqConnection(gatewayFactory, destinationSchema);

            ZmqJMSContext context = new ZmqJMSContext(connection, sessionMode);

            return context;
        } catch (ZmqException ex) {
            throw new JMSRuntimeException("Unable to create context.", ex.getErrorCode(), ex);
        }
    }
}
