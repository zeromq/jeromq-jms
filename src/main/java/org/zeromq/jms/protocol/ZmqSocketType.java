package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.ZMQ;

/**
 * Define enumerations for the Zero MQ socket types.
 */
public enum ZmqSocketType {
    PAIR(ZMQ.PAIR),

    /* Publish/Subscribe */
    PUB(ZMQ.PUB), SUB(ZMQ.SUB),

    /* Request/Response */
    REQ(ZMQ.REQ), REP(ZMQ.REP),

    /* Dealer/Router */
    DEALER(ZMQ.DEALER), ROUTER(ZMQ.ROUTER),

    /* Push/Pull */
    PULL(ZMQ.PULL), PUSH(ZMQ.PUSH),

    XPUB(ZMQ.XPUB), XSUB(ZMQ.XSUB);

    private final int type;

    /**
     * Construct the association between ENUM and ZMQ number constants.
     * @param type  the ZMQ constant
     */
    ZmqSocketType(final int type) {
        this.type = type;
    }

    /**
     * @return  return the underlying ZMQ number constants
     */
    public int getType() {
        return type;
    }
}
