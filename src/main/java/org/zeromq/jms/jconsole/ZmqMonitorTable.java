package org.zeromq.jms.jconsole;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.text.DateFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;

import org.zeromq.jms.jmx.ZmqGatewayManagerMBean;
import org.zeromq.jms.jmx.ZmqMBeanUtils;
import org.zeromq.jms.jmx.ZmqMetricsMBean;
import org.zeromq.jms.jmx.ZmqSocketStatisticsMBean;

/**
 * Construct the gateway/socket MBean Monitor panel.
 */
public class ZmqMonitorTable extends JPanel {

    private static final long serialVersionUID = 3865700192774656355L;

    private final ZmqMonitorTableModel model = new ZmqMonitorTableModel();

    private MBeanServerConnection mbConnection;

    /**
     * Inner case to render cell value for a specific format.
     */
    private static class FormatRenderer extends DefaultTableCellRenderer {
        private static final long serialVersionUID = -415071752821954019L;
        private final Format format;

        /**
         * Construct renderer for a format.
         * @param format  the format
         */
        FormatRenderer(final Format format) {
            this.format = format;

            if (format instanceof NumberFormat) {
                setHorizontalAlignment(SwingConstants.RIGHT);
            } else if (format instanceof DateFormat) {
                setHorizontalAlignment(SwingConstants.CENTER);
            }
        }

        @Override
        public void setValue(final Object value) {
            Object formattedValue;

            // Format the Object before setting its value in the renderer
            try {
                formattedValue = (value == null) ? null : format.format(value);
            } catch (IllegalArgumentException e) {
                formattedValue = value;
            }

            super.setValue(formattedValue);
        }
    }

    /**
     * Inner case to render cell value for with a background traffic light colour.
     */
    public static class TrafficLightRender extends DefaultTableCellRenderer {
        private static final long serialVersionUID = -8226481938945715389L;

        @Override
        public Component getTableCellRendererComponent(final JTable table, final Object value, final boolean isSelected, final boolean hasFocus,
                final int rowIndex, final int colIndex) {
            final JLabel label = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, hasFocus, rowIndex, colIndex);

            ZmqMonitorTableModel tableModel = (ZmqMonitorTableModel) table.getModel();

            if (tableModel.isActive(rowIndex)) {
                label.setBackground(Color.GREEN);
            } else {
                label.setBackground(Color.RED);
            }

            return label;
        }
    }

    /**
     * Construct the table panel (without table data).
     */
    public ZmqMonitorTable() {
        final JLabel label = new JLabel("Gateway/Sockets", JButton.CENTER);
        final JTable table = new JTable(model);

        for (int i = 0; i < table.getColumnModel().getColumnCount(); i++) {
            final String columnName = ZmqMonitorTableModel.COLUMN_NAMES[i];
            final Format columnFormat = ZmqMonitorTableModel.COLUMN_FORMATS[i];

            if ("Name".equals(columnName)) {
                final TrafficLightRender cellRenderer = new TrafficLightRender();
                table.getColumnModel().getColumn(i).setCellRenderer(cellRenderer);
            } else if (columnFormat != null) {
                final TableCellRenderer cellRenderer = new FormatRenderer(columnFormat);
                table.getColumnModel().getColumn(i).setCellRenderer(cellRenderer);

                if (columnFormat instanceof DateFormat) {
                    final String value = columnFormat.format(new Date());
                    final Component cell = cellRenderer.getTableCellRendererComponent(table, value, false, false, 0, i);
                    final int width = cell.getPreferredSize().width + table.getIntercellSpacing().width;

                    table.getColumnModel().getColumn(i).setMinWidth(width);
                }
            }
        }

        final JScrollPane scrollPane = new JScrollPane();
        scrollPane.getViewport().add(table);

        final JButton button = new JButton("Refresh");
        button.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent event) {
                initComponents();
            }
        });

        final JPanel content = new JPanel(new BorderLayout());

        content.add(label, BorderLayout.PAGE_START);
        content.add(scrollPane, BorderLayout.CENTER);
        content.add(button, BorderLayout.PAGE_END);

        this.add(content);
        this.setVisible(true);
    }

    /**
     * Setter for the JMX server connection.
     * @param connection   the server connection
     */
    public void setConnection(final MBeanServerConnection connection) {
        this.mbConnection = connection;
    }

    /**
     * re-initialize the table row data.
     */
    public void initComponents() {
        try {
            final List<ZmqMetricsMBean> rows = getMonitorTableRows(mbConnection);

            model.setRow(rows);
            model.fireTableDataChanged();
        } catch (IOException | MalformedObjectNameException ex) {
            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    JOptionPane.showMessageDialog(ZmqMonitorTable.this, "Cannot access the ZMQ MXBean in the target VM: " + ex.getMessage(),
                            "ZmqMonitorTable", JOptionPane.ERROR_MESSAGE);
                }
            });
        }
    }

    /**
     * Request the table row data based on ZMQ MBeans.
     * @param mbConnection                   the MBean connection
     * @return                               return the a list of row elements for the table
     * @throws IOException                   throws I/O exception on MBean connection failures
     * @throws MalformedObjectNameException  throw exception when MBean(s) cannot be found
     */
    private List<ZmqMetricsMBean> getMonitorTableRows(final MBeanServerConnection mbConnection) throws IOException, MalformedObjectNameException {

        final List<ZmqMetricsMBean> rows = new ArrayList<ZmqMetricsMBean>();
        final List<ZmqGatewayManagerMBean> gatewayMBeanList = ZmqMBeanUtils.getGatewayManagerMBeans(mbConnection);

        for (ZmqGatewayManagerMBean gatewayMBean : gatewayMBeanList) {
            rows.add(gatewayMBean);

            final String gatewayName = gatewayMBean.getName();
            final List<ZmqSocketStatisticsMBean> socketMBeanList = ZmqMBeanUtils.getSocketStatisticsMBeans(mbConnection, gatewayName);

            for (ZmqSocketStatisticsMBean socketMBean : socketMBeanList) {
                rows.add(socketMBean);
            }
        }

        return rows;
    }

}
