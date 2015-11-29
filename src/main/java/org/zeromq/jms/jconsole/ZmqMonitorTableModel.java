package org.zeromq.jms.jconsole;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.table.AbstractTableModel;

import org.zeromq.jms.jmx.ZmqGatewayManagerMBean;
import org.zeromq.jms.jmx.ZmqMetricsMBean;
import org.zeromq.jms.jmx.ZmqSocketStatisticsMBean;

/**
 * Table Model for the ZMQ Gateway and Socket table rows.
 */
public class ZmqMonitorTableModel extends AbstractTableModel {
    private static final long serialVersionUID = -7662927243300887754L;

    public static final String[] COLUMN_NAMES = new String[] { "Name", "Flags", "Start Time", "Last Send", "Send Rate (s)", "Send Max Rate (s)",
            "Send (30s)", "Send (60s)", "Send (5min)", "Send", "Last Rec", "Rec Rate (s)", "Rec Max Rate (s)", "Rec (30s)", "Rec (60s)",
            "Rec (5min)", "Rec" };

    public static final Format[] COLUMN_FORMATS = new Format[] { null, null, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("HH:mm:ss.SSS"), new DecimalFormat("0.00"), new DecimalFormat("0.00"), new DecimalFormat("0"),
            new DecimalFormat("0"), new DecimalFormat("0"), new DecimalFormat("0"), new SimpleDateFormat("HH:mm:ss.SSS"), new DecimalFormat("0.00"),
            new DecimalFormat("0.00"), new DecimalFormat("0"), new DecimalFormat("0"), new DecimalFormat("0"), new DecimalFormat("0") };

    private final Map<String, String> columnToMetricMap = new HashMap<String, String>();
    private List<ZmqMetricsMBean> rows = new ArrayList<ZmqMetricsMBean>();

    /**
     * Construction of the monitor table model.
     */
    public ZmqMonitorTableModel() {
        columnToMetricMap.put("Send Rate (s)", ZmqMetricsMBean.RATE_PER_SECOND);
        columnToMetricMap.put("Send Rate (m)", ZmqMetricsMBean.RATE_PER_MINUTE);
        columnToMetricMap.put("Send Max Rate (s)", ZmqMetricsMBean.MAX_RATE_PER_SECOND);
        columnToMetricMap.put("Send (30s)", ZmqMetricsMBean.COUNT_30_SECONDS);
        columnToMetricMap.put("Send (60s)", ZmqMetricsMBean.COUNT_60_SECONDS);
        columnToMetricMap.put("Send (90s)", ZmqMetricsMBean.COUNT_90_SECONDS);
        columnToMetricMap.put("Send (2min)", ZmqMetricsMBean.COUNT_2_MINUTES);
        columnToMetricMap.put("Send (5min)", ZmqMetricsMBean.COUNT_5_MINUTES);
        columnToMetricMap.put("Send (10min)", ZmqMetricsMBean.COUNT_10_MINUTES);
        columnToMetricMap.put("Send (30min)", ZmqMetricsMBean.COUNT_30_MINUTES);
        columnToMetricMap.put("Send (1h)", ZmqMetricsMBean.COUNT_1_HOUR);
        columnToMetricMap.put("Send (6h)", ZmqMetricsMBean.COUNT_6_HOURS);
        columnToMetricMap.put("Send (12h)", ZmqMetricsMBean.COUNT_12_HOURS);
        columnToMetricMap.put("Send (24h)", ZmqMetricsMBean.COUNT_24_HOURS);

        columnToMetricMap.put("Rec Rate (s)", ZmqMetricsMBean.RATE_PER_SECOND);
        columnToMetricMap.put("Rec Rate (m)", ZmqMetricsMBean.RATE_PER_MINUTE);
        columnToMetricMap.put("Rec Max Rate (s)", ZmqMetricsMBean.MAX_RATE_PER_SECOND);
        columnToMetricMap.put("Rec (30s)", ZmqMetricsMBean.COUNT_30_SECONDS);
        columnToMetricMap.put("Rec (60s)", ZmqMetricsMBean.COUNT_60_SECONDS);
        columnToMetricMap.put("Rec (90s)", ZmqMetricsMBean.COUNT_90_SECONDS);
        columnToMetricMap.put("Rec (2min)", ZmqMetricsMBean.COUNT_2_MINUTES);
        columnToMetricMap.put("Rec (5min)", ZmqMetricsMBean.COUNT_5_MINUTES);
        columnToMetricMap.put("Rec (10min)", ZmqMetricsMBean.COUNT_10_MINUTES);
        columnToMetricMap.put("Rec (30min)", ZmqMetricsMBean.COUNT_30_MINUTES);
        columnToMetricMap.put("Rec (1h)", ZmqMetricsMBean.COUNT_1_HOUR);
        columnToMetricMap.put("Rec (6h)", ZmqMetricsMBean.COUNT_6_HOURS);
        columnToMetricMap.put("Rec (12h)", ZmqMetricsMBean.COUNT_12_HOURS);
        columnToMetricMap.put("Rec (24h)", ZmqMetricsMBean.COUNT_24_HOURS);
    }

    /**
     * Set the models rows for refresh.
     * @param rows  the rows
     */
    public void setRow(final List<ZmqMetricsMBean> rows) {
        this.rows = rows;
    }

    /**
     * Static of underlying gateway/socket row.
     * @param rowIndex  the row index
     * @return          returh true when active
     */
    public boolean isActive(final int rowIndex) {
        final ZmqMetricsMBean mBean = rows.get(rowIndex);
        return mBean.isActive();
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    @Override
    public String getColumnName(final int columnIndex) {
        return COLUMN_NAMES[columnIndex];
    }

    @Override
    public Class<? extends Object> getColumnClass(final int columnIndex) {
        Object value = getValueAt(0, columnIndex);

        return value != null ? value.getClass() : String.class;
    }

    @Override
    public Object getValueAt(final int rowIndex, final int columnIndex) {
        final Object mBean = rows.get(rowIndex);

        if (mBean instanceof ZmqGatewayManagerMBean) {
            return getValue((ZmqGatewayManagerMBean) mBean, columnIndex);
        } else if (mBean instanceof ZmqSocketStatisticsMBean) {
            return getValue((ZmqSocketStatisticsMBean) mBean, columnIndex);
        } else {
            return false;
        }
    }

    /**
     * Return the column value based on a underlying socket MBean row.
     * @param socketMBean  the socket MBean
     * @param columnIndex  the column index
     * @return             return the column value based for the MBean attribute
     */
    protected Object getValue(final ZmqSocketStatisticsMBean socketMBean, final int columnIndex) {
        Object value = null;
        final String columnName = COLUMN_NAMES[columnIndex];
        switch (columnName) {
        case "Name":
            value = "   " + socketMBean.getAddr();
            break;

        case "Active":
            value = socketMBean.isActive();
            break;

        case "Send":
            value = socketMBean.getSendCount();
            break;

        case "Last Send":
            value = socketMBean.getLastSendTime();
            break;

        case "Rec":
            value = socketMBean.getReceiveCount();
            break;

        case "Last Rec":
            value = socketMBean.getLastReceiveTime();
            break;

        default:
            if (columnToMetricMap.containsKey(columnName)) {
                final String metricName = columnToMetricMap.get(columnName);

                if (columnName.startsWith("Send")) {
                    value = socketMBean.getSendMetrics().get(metricName);
                } else if (columnName.startsWith("Rec")) {
                    value = socketMBean.getReceiveMetrics().get(metricName);
                }
            }
        }

        return value;
    }

    /**
     * Return the column value based on a underlying gateway MBean row.
     * @param gatewayMBean  the gateway MBean
     * @param columnIndex   the column index
     * @return              return the column value based for the MBean attribute
     */
    protected Object getValue(final ZmqGatewayManagerMBean gatewayMBean, final int columnIndex) {

        Object value = null;
        final String columnName = COLUMN_NAMES[columnIndex];

        switch (columnName) {
        case "Name":
            value = gatewayMBean.getName();
            break;

        case "Active":
            value = gatewayMBean.isActive();
            break;

        case "Flags":
            StringBuilder flagsBuilder = new StringBuilder(gatewayMBean.getDirection().subSequence(0, 1));

            if (gatewayMBean.isBound()) {
                flagsBuilder.append(",B");
            }
            if (gatewayMBean.isAcknowledged()) {
                flagsBuilder.append(",A");
            }
            if (gatewayMBean.isHeartbeat()) {
                flagsBuilder.append(",H");
            }
            if (gatewayMBean.isTransacted()) {
                flagsBuilder.append(",T");
            }
            value = flagsBuilder.toString();
            break;

        case "Start Time":
            value = gatewayMBean.getStartTime();
            break;

        case "Send":
            value = gatewayMBean.getSendCount();
            break;

        case "Last Send":
            value = gatewayMBean.getLastSendTime();
            break;

        case "Rec":
            value = gatewayMBean.getReceiveCount();
            break;

        case "Last Rec":
            value = gatewayMBean.getLastReceiveTime();
            break;

        default:
        }

        return value;
    }
}
