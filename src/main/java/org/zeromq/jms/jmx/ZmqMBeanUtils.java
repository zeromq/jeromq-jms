package org.zeromq.jms.jmx;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.zeromq.jms.protocol.ZmqGateway;
import org.zeromq.jms.protocol.ZmqSocketMetrics;
import org.zeromq.jms.protocol.ZmqSocketType;

/**
 *  Utility class for querying for ZeroMQ MBeans.
 */
public class ZmqMBeanUtils {

    private static final Logger LOGGER = Logger.getLogger(ZmqMBeanUtils.class.getCanonicalName());

    private static final Map<ObjectName, Integer> REGISTERED_NAMES = new HashMap<ObjectName, Integer>();

    /**
     * Stop construction of utility class.
     */
    private ZmqMBeanUtils() {
    };

    /**
     * Return a list of ZMQ MBean instances.
     * @param  mBeanServerConnection        the MBean server open connection
     * @return                               return the current list ZMQ object instances
     * @throws MalformedObjectNameException  throws malformed object name exception
     * @throws IOException                   throws I/O exception
     */
    public static Set<ObjectInstance> getMbeans(final MBeanServerConnection mBeanServerConnection) throws MalformedObjectNameException, IOException {

        final ObjectName objectQueryName = new ObjectName("org.zeromq:type=ZMQ,*");
        final Set<ObjectInstance> objectInstances = mBeanServerConnection.queryMBeans(objectQueryName, null);

        return objectInstances;
    }

    /**
     * Returns a list of PROXY MBeans based on a query of the MBean Server for all Zero MQ gateway manager
     * MBeans. An empty list will be returned when non can be found.
     * @param  mBeanServerConnection         the MBean server open connection
     * @return                               return the current list of PROXY Zero MQ gateway manager MBeans
     * @throws MalformedObjectNameException  throws malformed object name exception
     * @throws IOException                   throws I/O exception
     */
    public static List<ZmqGatewayManagerMBean> getGatewayManagerMBeans(final MBeanServerConnection mBeanServerConnection)
            throws MalformedObjectNameException, IOException {

        final List<ZmqGatewayManagerMBean> managerMBeanList = new LinkedList<ZmqGatewayManagerMBean>();

        final ObjectName objectQueryName = new ObjectName("org.zeromq:type=ZMQ,*");
        final Set<ObjectInstance> objectInstances = mBeanServerConnection.queryMBeans(objectQueryName, null);

        for (ObjectInstance objectInstance : objectInstances) {
            final ObjectName objectName = objectInstance.getObjectName();
            final String socketName = objectName.getKeyProperty("socket");

            if (socketName == null) {
                final ZmqGatewayManagerMBean proxyMBean = JMX.newMBeanProxy(mBeanServerConnection, objectName, ZmqGatewayManagerMBean.class);

                managerMBeanList.add(proxyMBean);
            }
        }

        return managerMBeanList;
    }

    /**
     * Returns a list of PROXY MBeans based on a query of the MBean Server for all Zero MQ gateway manager
     * MBeans. The list is further filter by MBean name using the specified filter.
     * @param  mBeanServerConnection         the MBean server open connection
     * @param  namePattern                   the filter REGX pattern used against the MBean name attribute
     * @return                               return the current list of PROXY Zero MQ gateway manager MBeans
     * @throws MalformedObjectNameException  throws malformed object name exception
     * @throws IOException                   throws I/O exception
     */
    public static List<ZmqGatewayManagerMBean> getGatewayManagerMBeans(final MBeanServerConnection mBeanServerConnection, final String namePattern)
            throws MalformedObjectNameException, IOException {

        final List<ZmqGatewayManagerMBean> managerMBeanList = getGatewayManagerMBeans(mBeanServerConnection);
        final List<ZmqGatewayManagerMBean> managerMBeanFilterList = new LinkedList<ZmqGatewayManagerMBean>();

        for (ZmqGatewayManagerMBean managerMBean : managerMBeanList) {
            final String name = managerMBean.getName();

            if (name.matches(namePattern)) {
                managerMBeanFilterList.add(managerMBean);
            }
        }

        return managerMBeanFilterList;
    }

    /**
     * Returns a list of PROXY MBeans based on a queries of the MBean Server for all Zero MQ socket statistics
     * MBeans. An empty list will be returned when non can be found.
     * @param  mBeanServerConnection        the MBean server open connection
     * @param  gatewayName                  the MBean gateway name
     * @return                               return the current list of PROXY Zero MQ socket statistics MBeans
     * @throws MalformedObjectNameException  throws malformed object name exception
     * @throws IOException                   throws I/O exception
     */
    public static List<ZmqSocketStatisticsMBean> getSocketStatisticsMBeans(final MBeanServerConnection mBeanServerConnection,
            final String gatewayName) throws MalformedObjectNameException, IOException {

        final List<ZmqSocketStatisticsMBean> statisticsMBeanList = new LinkedList<ZmqSocketStatisticsMBean>();

        final ObjectName objectQueryName = new ObjectName("org.zeromq:type=ZMQ,gateway=\"" + gatewayName + "\",socket=*");
        final Set<ObjectInstance> objectInstances = mBeanServerConnection.queryMBeans(objectQueryName, null);

        for (ObjectInstance objectInstance : objectInstances) {
            final ObjectName objectName = objectInstance.getObjectName();
            final ZmqSocketStatisticsMBean proxyMBean = JMX.newMBeanProxy(mBeanServerConnection, objectName, ZmqSocketStatisticsMBean.class);

            statisticsMBeanList.add(proxyMBean);
        }

        return statisticsMBeanList;
    }

    /**
     * Create and register an statistic MBean for the specified gateway.
     * @param gateway   the gateway to be measured
     * @return          return the MBean object name that were registered
     */
    public static List<ObjectName> register(final ZmqGateway gateway) {
        final List<ObjectName> objectNames = new LinkedList<ObjectName>();

        boolean isRegister = false;
        int version = 0;
        String gatewayName = null;
        ObjectName gatewayObjectName = null;

        try {
            do {
                final String registeredVersion = (isRegister) ? "[" + (++version) + "]" : "";
                gatewayName = gateway.getName().replace(":", "").replace("/", "").replace("*", "") + registeredVersion;
                final String gatewayMBeanName = "org.zeromq:type=ZMQ,gateway=\"" + gatewayName + "\"";
                gatewayObjectName = new ObjectName(gatewayMBeanName);
                isRegister = REGISTERED_NAMES.containsKey(gatewayObjectName);

                if (isRegister) {
                    version = REGISTERED_NAMES.get(gatewayObjectName);
                }
            } while (isRegister);

            final ZmqGatewayManager gatewayMBean = new ZmqGatewayManager(gatewayName, gateway);
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(gatewayMBean, gatewayObjectName);

            REGISTERED_NAMES.put(gatewayObjectName, version);

            objectNames.add(gatewayObjectName);

            final boolean isBound = gateway.isBound();
            final ZmqSocketType type = gateway.getType();

            final List<ZmqSocketMetrics> metrics = gateway.getMetrics();

            for (ZmqSocketMetrics metric : metrics) {
                final String socketName = gatewayName + "@" + metric.getAddr().replace(":", "").replace("/", "").replace("*", "");
                final ZmqSocketStatistics statisticsMBean = new ZmqSocketStatistics(socketName, metric.getAddr(), type.toString(), isBound, metric);
                final String socketMBeanName = "org.zeromq:type=ZMQ,gateway=\"" + gatewayName + "\",socket=\"" + socketName + "\"";
                final ObjectName socketObjectName = new ObjectName(socketMBeanName);
                mbs.registerMBean(statisticsMBean, socketObjectName);

                objectNames.add(socketObjectName);
                REGISTERED_NAMES.put(socketObjectName, version);
            }
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException ex) {
            LOGGER.log(Level.SEVERE, "Unable to register MBean: " + gatewayName, ex);
        }

        return objectNames;
    }

    /**
     * Unregister the MBean from the registry.
     * @param objectName  the name of the MBean to unregister
     */
    public static void unregister(final ObjectName objectName) {
        try {
            REGISTERED_NAMES.remove(objectName);

            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            mbs.unregisterMBean(objectName);
        } catch (InstanceNotFoundException | MBeanRegistrationException ex) {
            LOGGER.log(Level.SEVERE, "Unable to unregister MBean: " + objectName, ex);
        }
    }

}
