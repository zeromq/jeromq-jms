package org.zeromq.jms.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

/*
 * Copyright (c) 2019 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * Class to pool the ZMQ context to ensure "inproc" share the same instance, otherwise\
 * it does not work.
 */
public final class ZmqContextPool {
    private static final Logger LOGGER = Logger.getLogger(ZmqContextPool.class.getCanonicalName());

    /**
     * Context reference.
     */
    private static class Reference {
        private Context context;
        private int count;
    }

    private static final Map<String, Reference> CONTEXT_POOL = new HashMap<String, Reference>();

    /**
     * Not possible utility class constructor.
     */
    private ZmqContextPool() {
        // Utility class.
    }

    /**
     * Create a new context when one does not exist, otherwise return the existing context.
     * @param name       the name of the context
     * @param ioThreads  number of threads on creating a context
     * @return           return the new or existing context
     */
    public static synchronized ZMQ.Context getContext(final String name, final int ioThreads) {
        if (!CONTEXT_POOL.containsKey(name)) {
            final Reference reference = new Reference();

            reference.context = ZMQ.context(ioThreads);
            reference.count = 0;

            CONTEXT_POOL.put(name, reference);
        }

        final Reference reference = CONTEXT_POOL.get(name);

        reference.count++;

        LOGGER.finest("Get ZMQ.Context[name=" + name + ", count=" + reference.count + "] from pool");

        return reference.context;
    }

    /**
     * Release the context, and close the context when no more open connections.
     * @param name  the context to release/close
     * @return      return context that has been released
     */
    public static synchronized ZMQ.Context releaseContext(final String name) {
        final Reference reference = CONTEXT_POOL.get(name);

        if (reference == null) {
            return null;
        }

        reference.count--;

        if (reference.count == 0) {
            CONTEXT_POOL.remove(name);

            if (!reference.context.isClosed()) {
                reference.context.close();
            }
        }

        LOGGER.finest("Release ZMQ.Context[name=" + name + ", count=" + reference.count + "] from pool");

        return reference.context;
    }
}
