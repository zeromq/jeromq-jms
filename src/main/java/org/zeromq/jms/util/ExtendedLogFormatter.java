package org.zeromq.jms.util;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * SimpleFormatter does not have the thread, so need to create a new formatter to expose the thread.
 */
public class ExtendedLogFormatter extends Formatter {
    private static final MessageFormat MESSAGE_FORMAT = new MessageFormat("{0,date,yyyy-mm-dd} {0,time,HH:mm:ss.SSS} {1} --- [{2}] {3} : {4}\n");

    @Override
    public String format(final LogRecord record) {

        Object[] arguments = new Object[6];
        arguments[0] = new Date(record.getMillis());
        arguments[1] = String.format("%1$7s", record.getLevel().getName());
        arguments[2] = String.format("%1$15s", Thread.currentThread().getName());
        arguments[3] = String.format("%1$-60s", compactClassName(record.getSourceClassName()) + "." + record.getSourceMethodName());
        arguments[4] = record.getMessage();

        return MESSAGE_FORMAT.format(arguments);
    }

    /**
     * Create a compact package path in front of the class name.
     * @param canonicalName  the canonical name  class name (include package path)
     * @return               return the compacted name
     */
    private String compactClassName(final String canonicalName) {
        final StringBuilder builder = new StringBuilder();

        int prevPos = 0;
        int nextPos = 0;

        do {
            nextPos = canonicalName.indexOf(".", nextPos + 1);

            if (nextPos > 0) {
                builder.append(canonicalName.charAt(prevPos)).append(".");
                prevPos = nextPos + 1;
            }
        } while (nextPos > 0);

        builder.append(canonicalName.substring(prevPos));

        return builder.toString();
    }
}
