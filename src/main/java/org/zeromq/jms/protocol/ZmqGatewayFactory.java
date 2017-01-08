package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Queue;

import org.zeromq.ZMQ;
import org.zeromq.jms.AbstractZmqDestination;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqSession;
import org.zeromq.jms.ZmqURI;
import org.zeromq.jms.annotation.ClassUtils;
import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.event.ZmqStompEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.protocol.filter.ZmqFixedFilterPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRedeliveryPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRetryRedeliveryPolicy;
import org.zeromq.jms.protocol.store.ZmqJournalStore;
import org.zeromq.jms.selector.ZmqMessageSelector;
import org.zeromq.jms.selector.ZmqMessageSelectorFactory;

/**
 * Factory class for returning a protocol instance.
 */
public class ZmqGatewayFactory {
    private static final Logger LOGGER = Logger.getLogger(ZmqSession.class.getCanonicalName());

    private final Class<?> defaultGateway = ZmqFireAndForgetGateway.class;
    private final ZmqMessageSelectorFactory defaultSelectorFactory = new ZmqMessageSelectorFactory();
    private final ZmqEventHandler defaultEventHandler = new ZmqStompEventHandler();
    private final ZmqFilterPolicy defaultFilterPolicy = new ZmqFixedFilterPolicy();
    private final ZmqRedeliveryPolicy defaultRedeliveryPolicy = new ZmqRetryRedeliveryPolicy(3);

    private final Map<String, ZmqURI> destinationSchema;

    private final List<Class<?>> gatewayClasses;
    private final List<Class<?>> eventHandlerClasses;
    private final List<Class<?>> filterPolicyClasses;
    private final List<Class<?>> redeliveryPolicyClasses;
    private final List<Class<?>> journalStoreClasses;

    /**
     * Construct the protocol factory around the destination schema.
     * @param extensionPackageNames  the array of package names to find extensions
     * @param destinationSchema      the schema of known destinations
     */
    public ZmqGatewayFactory(final String[] extensionPackageNames, final Map<String, ZmqURI> destinationSchema) {
        this.destinationSchema = destinationSchema;

        this.gatewayClasses = getClasses(extensionPackageNames, ZmqGateway.class);
        this.eventHandlerClasses = getClasses(extensionPackageNames, ZmqEventHandler.class);
        this.filterPolicyClasses = getClasses(extensionPackageNames, ZmqFilterPolicy.class);
        this.redeliveryPolicyClasses = getClasses(extensionPackageNames, ZmqRedeliveryPolicy.class);
        this.journalStoreClasses = getClasses(extensionPackageNames, ZmqJournalStore.class);
    }

    /**
     * Return all classes below the package roots that have the specified component class.
     * @param packageNames             the starting packages to search.
     * @param componentInterface       the component interface to find
     * @return                         return a list of classes found (an empty list is possible)
     */
    public static List<Class<?>> getClasses(final String[] packageNames, final Class<?> componentInterface) {
        final List<Class<?>> annotatedClasses = new LinkedList<Class<?>>();

        try {
            final List<Class<?>> mainClasses = ClassUtils.getClasses("org.zeromq.jms");

            for (Class<?> clazz : mainClasses) {
                if (clazz.isAnnotationPresent(ZmqComponent.class) && componentInterface.isAssignableFrom(clazz)) {
                    annotatedClasses.add(clazz);
                }
            }

            if (packageNames != null) {
                for (String packageName : packageNames) {
                    if (!packageName.startsWith("org.zeromq.jms")) {
                        final List<Class<?>> extensionClasses = ClassUtils.getClasses(packageName);

                        for (Class<?> clazz : extensionClasses) {
                            if (clazz.isAnnotationPresent(ZmqComponent.class) && componentInterface.isAssignableFrom(clazz)) {
                                annotatedClasses.add(clazz);
                            }
                        }
                    }
                }
            }
        } catch (IOException | ClassNotFoundException ex) {
            LOGGER.log(Level.SEVERE, "Unable to scan classes for ZMQ annotations.", ex);
        }

        return annotatedClasses;
    }

    /**
     * Return the Zero MQ JMS consumer protocol based on the attributes and any meta data.
     * @param  namePrefix       the prefix name of the producer gateway
     * @param  destination      the destination
     * @param  context          the ZMQ context
     * @param  type             the ZMQ socket type as enum, i.e. PUB, SUB, etc...
     * @param  isBound          the connect or bind to the socket
     * @param  messageSelector  the JMS select expression
     * @param  transacted       the transaction indicator
     * @return                  return the constructed gateway
     * @throws ZmqException     throw JMS exception when filters cannot be resolved
     */
    public ZmqGateway newConsumerGateway(final String namePrefix, final AbstractZmqDestination destination, final ZMQ.Context context,
            final ZmqSocketType type, final boolean isBound, final String messageSelector, final boolean transacted) throws ZmqException {

        final String destinationName = destination.getName();
        final ZmqURI destinationUri = destination.getURI();
        final int flags = 0;

        final ZmqMessageSelector selector = getZmqMessageSelector(destination, messageSelector);
        final ZmqRedeliveryPolicy redelivery = null;
        final ZmqEventHandler eventHandler = getZmqEventHandler(destination);
        final ZmqFilterPolicy filter = getZmqFilterPolicy(destination);
        final ZmqJournalStore store = getZmqJournalStore(destination, ZmqGateway.Direction.INCOMING);

        try {
            final ZmqURI uri = (destinationUri == null) ? destinationSchema.get(destinationName) : destinationUri;

            if (uri == null) {
                throw new ZmqException("Missing URI to construct gateway consumer: " + destination);
            }

            final String value = uri.getOptionValue("gateway", null);

            Class<?> gatewayClass = defaultGateway;

            if (value != null) {
                gatewayClass = ClassUtils.getClass(gatewayClasses, ZmqComponent.class, "value", value);

                if (gatewayClass == null) {
                    throw new ZmqException("Unable to find specified gateway: " + value);
                }
            }

            if (gatewayClass != null) {
                LOGGER.info("Using gateway consumer  (" + gatewayClass.getClass().getCanonicalName() + ") for destination: " + destination);
            }

            final Constructor<?> consumerConstructor = gatewayClass.getConstructor(String.class, ZMQ.Context.class, ZmqSocketType.class,
                    boolean.class, String.class, int.class, ZmqFilterPolicy.class, ZmqEventHandler.class, ZmqGatewayListener.class,
                    ZmqJournalStore.class, ZmqMessageSelector.class, ZmqRedeliveryPolicy.class,
                    boolean.class, ZmqGateway.Direction.class);

            final boolean socketBound = uri.getOptionValue("gateway.bind", isBound);
            final ZmqSocketType socketType = ZmqSocketType.valueOf(uri.getOptionValue("gateway.type", type.toString()));
            final String socketAddr = uri.getOptionValue("gateway.addr");

            if (socketAddr == null) {
                throw new ZmqException("Missing UTI 'gateway.addr' construct gateway consumer: " + uri);
            }

            final String name = namePrefix + "@" + socketAddr;
            final ZmqGateway protocol = (ZmqGateway) consumerConstructor.newInstance(name, context, socketType, socketBound, socketAddr, flags,
                    filter, eventHandler, null, store, selector, redelivery, 
                    transacted, ZmqGateway.Direction.INCOMING);

            if (uri != null) {
                final Map<String, List<String>> parameters = uri.getOptions();
                ClassUtils.setMethods(parameters, protocol);
            }

            return protocol;
        } catch (IllegalArgumentException | ReflectiveOperationException ex) {
            LOGGER.log(Level.SEVERE, "Unable to construct consumer based on URI: " + destinationUri, ex);

            throw new ZmqException("Unable to construct consumer based on URI: " + destinationUri, ex);
        }
    }

    /**
     * Return the Zero MQ JMS producer protocol based on the attributes and any meta data.
     * @param  namePrefix     the prefix name of the producer gateway
     * @param  destination    the destination
     * @param  context        the ZMQ context
     * @param  type           the ZMQ socket type as enum, i.e. PUB, SUB, etc...
     * @param  isBound        the connect or bind to the socket
     * @param  transacted     the transaction indicator
     * @return                return the constructed gateway
     * @throws ZmqException   throw JMS exception when filters cannot be resolved
     */
    public ZmqGateway newProducerGateway(final String namePrefix, final AbstractZmqDestination destination, final ZMQ.Context context,
            final ZmqSocketType type, final boolean isBound, final boolean transacted) throws ZmqException {

        final String destinationName = destination.getName();
        final ZmqURI destinationUri = destination.getURI();
        final int flags = 0;
        final ZmqMessageSelector selector = null;
        final ZmqRedeliveryPolicy redelivery = null;
        final ZmqEventHandler handler = getZmqEventHandler(destination);
        final ZmqFilterPolicy filter = getZmqFilterPolicy(destination);
        final ZmqGatewayListener listener = null;
        final ZmqJournalStore store = getZmqJournalStore(destination, ZmqGateway.Direction.OUTGOING);

        try {
            final ZmqURI uri = (destinationUri == null) ? destinationSchema.get(destinationName) : destinationUri;

            if (uri == null) {
                throw new ZmqException("Missing URI to construct gateway consumer: " + destination);
            }

            final String value = uri.getOptionValue("gateway");

            Class<?> gatewayClass = defaultGateway;

            if (value != null) {
                gatewayClass = ClassUtils.getClass(gatewayClasses, ZmqComponent.class, "value", value);
            }

            if (gatewayClass != null) {
                LOGGER.info("Using gateway produce  (" + gatewayClass.getClass().getCanonicalName() + ") for destination: " + destination);
            }

            final Constructor<?> producerConstructor = gatewayClass.getConstructor(String.class, ZMQ.Context.class, ZmqSocketType.class,
                    boolean.class, String.class, int.class, ZmqFilterPolicy.class, ZmqEventHandler.class, ZmqGatewayListener.class,
                    ZmqJournalStore.class, ZmqMessageSelector.class, ZmqRedeliveryPolicy.class,
                    boolean.class, ZmqGateway.Direction.class);

            final boolean socketBound = uri.getOptionValue("gateway.bind", isBound);
            final ZmqSocketType socketType = ZmqSocketType.valueOf(uri.getOptionValue("gateway.type", type.toString()));
            final String socketAddr = uri.getOptionValue("gateway.addr");

            if (socketAddr == null) {
                throw new ZmqException("Missing UTI 'gateway.addr' construct gateway consumer: " + uri);
            }

            final String name = namePrefix + "@" + socketAddr;
            final ZmqGateway protocol = (ZmqGateway) producerConstructor.newInstance(name, context, socketType, socketBound, socketAddr, flags,
                    filter, handler, listener, store, selector, redelivery,
                    transacted, ZmqGateway.Direction.OUTGOING);

            if (uri != null) {
                final Map<String, List<String>> parameters = uri.getOptions();
                ClassUtils.setMethods(parameters, protocol);
            }

            return protocol;
        } catch (IllegalArgumentException | ReflectiveOperationException ex) {
            LOGGER.log(Level.SEVERE, "Unable to construct producer based on URI: " + destinationUri, ex);

            throw new ZmqException("Unable to construct produce based on URI: " + destinationUri, ex);
        }
    }

    /**
     * Return the message selector based on the specified condition statement.
     * @param  destination    the destination.
     * @param  expression     the SQL style condition expression
     * @return                return the selector
     * @throws ZmqException   throw JMS exception when message selector cannot be resolved
     */
    protected ZmqMessageSelector getZmqMessageSelector(final AbstractZmqDestination destination, final String expression) throws ZmqException {
        if (expression == null || expression.trim().length() == 0) {
            return null;
        }

        final String name = destination.getName();

        try {
            ZmqMessageSelector selector = null;

            if (destinationSchema.containsKey(name)) {
                final ZmqURI uri = destinationSchema.get(name);
                final String value = uri.getOptionValue("selector", null);

                if (value != null) {
                    final Class<?> selectorFactoryClass = ClassUtils.getClass(eventHandlerClasses, ZmqComponent.class, "value", value);

                    if (selectorFactoryClass != null) {
                        final ZmqMessageSelectorFactory selectorFactory = (ZmqMessageSelectorFactory) selectorFactoryClass.newInstance();

                        LOGGER.info("Using selector factory (" + selectorFactory.getClass().getCanonicalName() + ") for destination: " + destination);

                        selector = selectorFactory.parse(expression);
                    }
                }
            }

            if (selector == null) {
                selector = defaultSelectorFactory.parse(expression);
                LOGGER.info("Using default selector factory (" + defaultSelectorFactory.getClass().getCanonicalName() + ") for destination: "
                        + destination);
            }

            if (destinationSchema.containsKey(name) && (selector != null)) {
                final ZmqURI uri = destinationSchema.get(name);
                final Map<String, List<String>> parameters = uri.getOptions();

                ClassUtils.setMethods(parameters, selector);
            }

            return selector;
        } catch (Exception ex) {
            throw new ZmqException("Unable resolve the message selector for destination: " + name, ex);
        }
    }

    /**
     * Return the event handler for this destination. When no specific event handler is found
     * then the default handler is used.
     * @param  destination    the destination.
     * @return                return the event handler
     * @throws ZmqException   throw JMS exception when event handler cannot be resolved
     */
    protected ZmqEventHandler getZmqEventHandler(final AbstractZmqDestination destination) throws ZmqException {
        final String name = destination.getName();

        try {
            ZmqEventHandler eventHandler = null;

            if (destinationSchema.containsKey(name)) {
                final ZmqURI uri = destinationSchema.get(name);
                final String value = uri.getOptionValue("event");

                if (value != null) {
                    final Class<?> eventHandlerClass = ClassUtils.getClass(eventHandlerClasses, ZmqComponent.class, "value", value);

                    if (eventHandlerClass == null) {
                        throw new ZmqException("Unable to find specified event handler: " + value);
                    } else {
                        eventHandler = (ZmqEventHandler) eventHandlerClass.newInstance();
                        LOGGER.info("Using event handler  (" + eventHandler.getClass().getCanonicalName() + ") for destination: " + destination);
                    }
                }
            }

            if (eventHandler == null) {
                eventHandler = defaultEventHandler;
                LOGGER.info("Using default event handler (" + eventHandler.getClass().getCanonicalName() + ") for destination: " + destination);
            }

            if (destinationSchema.containsKey(name) && (eventHandler != null)) {
                final ZmqURI uri = destinationSchema.get(name);
                final Map<String, List<String>> parameters = uri.getOptions();

                ClassUtils.setMethods(parameters, eventHandler);
            }

            return eventHandler;
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unable resolve the event handler for destination " + name, ex);

            throw new ZmqException("Unable resolve the event handler for destination: " + name, ex);
        }
    }

    /**
     * Return the subscribe filter policy. The are 2 parts, the String filter based on the underlying the
     * JMS messages when publishing, and subscription when consuming which is a list string filters.
     * NOTE: ZMQ Pub/Sub requires a valid string value to be used ("null" is not allowed), so the Fixed Filter
     *       policy always uses the none" string when no otherwise initialised.
     * @param  destination    the destination.
     * @return                return the subscribe resolver
     * @throws ZmqException   throw JMS exception when subscribe resolver cannot be resolved
     */
    protected ZmqFilterPolicy getZmqFilterPolicy(final AbstractZmqDestination destination) throws ZmqException {
        if (destination instanceof Queue) {
            return null;
        }

        final String name = destination.getName();

        try {
            ZmqFilterPolicy filter = null;

            if (destinationSchema.containsKey(name)) {
                final ZmqURI uri = destinationSchema.get(name);
                final String value = uri.getOptionValue("filter");

                if (value != null) {
                    final Class<?> filterPolicyClass = ClassUtils.getClass(filterPolicyClasses, ZmqComponent.class, "value", value);

                    if (filterPolicyClass == null) {
                        throw new ZmqException("Unable to find specified filter policy: " + value);
                    } else {
                        filter = (ZmqFilterPolicy) filterPolicyClass.newInstance();

                        LOGGER.info("Using filter policy  (" + filter.getClass().getCanonicalName() + ") for destination: " + destination);

                        final String[] filters = uri.getOptionValues("filter.value", null);

                        if (filters != null && filters.length > 0) {
                            filter.setFilters(null);

                            LOGGER.info("Using filters  (" + Arrays.toString(filters) + ") for destination: " + destination);
                        }
                    }
                }
            }

            if (filter == null) {
                filter = defaultFilterPolicy;

                if (filter == null) {
                    LOGGER.info("NULL default filter policy for destination: " + destination);
                } else {
                    LOGGER.info("Using default filter policy (" + defaultFilterPolicy.getClass().getCanonicalName() + ") for destination: "
                            + destination);
                }
            }

            if (destinationSchema.containsKey(name) && (filter != null)) {
                final ZmqURI uri = destinationSchema.get(name);
                final Map<String, List<String>> parameters = uri.getOptions();

                ClassUtils.setMethods(parameters, filter);
            }

            return filter;
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unable resolve the message filter policy for destination " + name, ex);

            throw new ZmqException("Unable resolve the message filter policy for destination: " + name, ex);
        }
    }

    /**
     * Return the defaulter message journal store for this destination.
     * @param  destination    the destination.
     * @param  direction      the direction of the queue
     * @return                return the re-delivery policy
     * @throws ZmqException   throw JMS exception when journal store cannot be resolved
     */
    protected ZmqJournalStore getZmqJournalStore(final AbstractZmqDestination destination, final ZmqGateway.Direction direction) throws ZmqException {
        final String name = destination.getName();

        try {
        	ZmqJournalStore store = null;

            if (destinationSchema.containsKey(name)) {
                final ZmqURI uri = destinationSchema.get(name);
                final String value = uri.getOptionValue("journal");

                if (value != null) {
                    final Class<?> journalStoreClass = ClassUtils.getClass(journalStoreClasses, ZmqComponent.class, "value", value);

                    if (journalStoreClass == null) {
                        throw new ZmqException("Unable to find specified journal store: " + value);
                    } else {
                        final Constructor<?> storeConstructor =
                        	journalStoreClass.getConstructor(Path.class, String.class, String.class);

                        final String groupId = name + "-" + direction.name().toLowerCase();
                		final String uniqueId = ManagementFactory.getRuntimeMXBean().getName().replaceAll("\\W+", "-").toLowerCase();
                
                        store = (ZmqJournalStore) storeConstructor.newInstance(null, groupId, uniqueId);

                        LOGGER.info("Using journal store  (" + store.getClass().getCanonicalName() + ") for destination: "
                                + destination);
                    }
                }

            }

            if (store == null) {
                LOGGER.info("Using NO jounral store for destination: " + destination);
            } 

            if (destinationSchema.containsKey(name) && (store != null)) {
                final ZmqURI uri = destinationSchema.get(name);
                final Map<String, List<String>> parameters = uri.getOptions();

                ClassUtils.setMethods(parameters, store);
            }

            return store;
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unable resolve the message journal store for destination " + name, ex);

            throw new ZmqException("Unable resolve the message journal store for destination: " + name, ex);
        }
    }

    /**
     * Return the defaulter re-delivery policy for this destination.
     * @param  destination    the destination.
     * @return                return the re-delivery policy
     * @throws ZmqException   throw JMS exception when redelivery policy cannot be resolved
     */
    protected ZmqRedeliveryPolicy getRedeliveryPolicy(final AbstractZmqDestination destination) throws ZmqException {
        final String name = destination.getName();

        try {
            ZmqRedeliveryPolicy redeliveryPolicy = null;

            if (destinationSchema.containsKey(name)) {
                final ZmqURI uri = destinationSchema.get(name);
                final String value = uri.getOptionValue("redelivery");

                if (value != null) {
                    final Class<?> redeliveryPolicyClass = ClassUtils.getClass(redeliveryPolicyClasses, ZmqComponent.class, "value", value);

                    if (redeliveryPolicyClass == null) {
                        throw new ZmqException("Unable to find specified re-delivery policy: " + value);
                    } else {
                        redeliveryPolicy = (ZmqRedeliveryPolicy) redeliveryPolicyClass.newInstance();

                        LOGGER.info("Using re-delivery policy  (" + redeliveryPolicy.getClass().getCanonicalName() + ") for destination: "
                                + destination);
                    }
                }
            }

            if (redeliveryPolicy == null) {
                redeliveryPolicy = defaultRedeliveryPolicy;

                LOGGER.info("Using default re-delivery policy (" + redeliveryPolicy.getClass().getCanonicalName() + ") for destination: "
                        + destination);
            }

            if (destinationSchema.containsKey(name) && (redeliveryPolicy != null)) {
                final ZmqURI uri = destinationSchema.get(name);
                final Map<String, List<String>> parameters = uri.getOptions();

                ClassUtils.setMethods(parameters, redeliveryPolicy);
            }

            return redeliveryPolicy;
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unable resolve the re-delivery policy for destination " + name, ex);

            throw new ZmqException("Unable resolve the re-delivery policy for destination: " + name, ex);
        }
    }
}
