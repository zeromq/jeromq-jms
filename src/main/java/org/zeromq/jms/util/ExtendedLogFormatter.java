package org.zeromq.jms.util;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * SimpleFormatter does not have the thread, so need to create a new formatter to expose the thread.
 */
public class ExtendedLogFormatter extends Formatter {

    @Override
    public String format(final LogRecord record) {

        Object[] arguments = new Object[6];
        arguments[0] = new Date(record.getMillis());
        arguments[1] = record.getLevel().getName();
        arguments[2] = Thread.currentThread().getName();
        arguments[3] = compactClassName(record.getSourceClassName()) + "." + record.getSourceMethodName();

        final Throwable thrown = record.getThrown();

        if (thrown == null) {
            arguments[4] = record.getMessage();
        } else {
            final StringWriter stringWriter = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(stringWriter);

            printWriter.println(record.getMessage());

            thrown.printStackTrace(printWriter);
            arguments[4] = stringWriter.toString();
        }

        return String.format("%1$tF %1$tT:%1$tL %2$7s --- [%3$17s] %4$-60s : %5$s\n", arguments);
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
