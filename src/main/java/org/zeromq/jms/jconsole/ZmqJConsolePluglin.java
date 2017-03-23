package org.zeromq.jms.jconsole;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import com.sun.tools.jconsole.JConsoleContext;
import com.sun.tools.jconsole.JConsolePlugin;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.swing.BoxLayout;
import javax.swing.JPanel;
import javax.swing.SwingWorker;

/**
 * Reference documentation.
 *
 *    http://www.javaworld.com/article/2072261/using-orson-chart-beans-with-jconsole-custom-plugin.html
 *    https://blogs.oracle.com/jmxetc/entry/a_beanshell_plugin_for_jconsole
 *
 * Running JConsole (including plug-in).
 *
 *    jconsole -pluginpath jeromq-jms.jar -J-Djava.class.path="%JAVA_HOME%\lib\jconsole.jar";"%JAVA_HOME%\lib\tools.jar"
 *    jconsole -J-Djava.class.path=./jconsole.jar:./tools.jar
 *
 * Java command line to run JConsole for debugging.

 *    java -classpath ./jconsole.jar:./tools.jar sun.tools.jconsole.JConsole
 *
 * Code examples.
 *
 *    https://bitbucket.org/pjtr/topthreads
 *    http://stackoverflow.com/questions/8693342/drawing-a-simple-line-graph-in-java
 */
//@SuppressWarnings("restriction")
public class ZmqJConsolePluglin extends JConsolePlugin implements PropertyChangeListener {

    private ZmqMonitorTable customPanel = null;
    private Map<String, JPanel> customTabs = null;

    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
    }

    @Override
    public Map<String, JPanel> getTabs() {
        if (customPanel == null) {
            customPanel = new ZmqMonitorTable();
        }

        if (customTabs == null) {
            customPanel.setLayout(new BoxLayout(customPanel, BoxLayout.PAGE_AXIS));
            customPanel.setVisible(true);

            customTabs = new LinkedHashMap<String, JPanel>();
            customTabs.put("ZeroMQ", customPanel);
        }

        final JConsoleContext ctx = getContext();

        if (ctx != null && JConsoleContext.ConnectionState.CONNECTED.equals(ctx.getConnectionState())) {
            MBeanServerConnection mbConnection = getContext().getMBeanServerConnection();

            if (mbConnection != null) {
                customPanel.setConnection(mbConnection);
            }
        }

        return customTabs;
    }

    @Override
    public SwingWorker<?, ?> newSwingWorker() {
        return new RefreshMbeanWorker();
    }

    /**
     * Inner class thread for MBean refresh.
     */
    private class RefreshMbeanWorker extends SwingWorker<Integer, String> {

        @Override
        protected Integer doInBackground() throws Exception {
            customPanel.initComponents();

            return 1;
        }

    }
}
