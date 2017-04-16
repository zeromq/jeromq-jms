package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRedeliveryPolicy;
import org.zeromq.jms.protocol.store.ZmqJournalStore;
import org.zeromq.jms.selector.ZmqMessageSelector;

/**
 * Fire a Forget gateway. Send message and forget for acknowledgements, fail-over, heart-beats, etc...
 */
@ZmqComponent("fireAndForget")
@ZmqUriParameter("gateway")
public class ZmqFireAndForgetGateway extends AbstractZmqGateway {

    /**
     * Construct the Fire and Forget gateway.
     * @param name              the name of display the gateway
     * @param socketContext     the socket context for the ZMQ socket
     * @param filter            the message filter policy
     * @param handler           the message event handler functionality
     * @param listener          the listener instance
     * @param store             the (optional) message store
     * @param selector          the (optional) message selection policy
     * @param redelivery        the (optional) message re-delivery policy
     * @param transacted        the transaction indicator
     * @param direction         the direction, i.e. Incoming, Outgoing, etc..
     */
    public ZmqFireAndForgetGateway(final String name, final ZmqSocketContext socketContext,
        final ZmqFilterPolicy filter, final ZmqEventHandler handler, final ZmqGatewayListener listener,
        final ZmqJournalStore store, final ZmqMessageSelector selector, final ZmqRedeliveryPolicy redelivery,
        final boolean transacted, final Direction direction) {

        super(name, socketContext,
           filter, handler, listener, store, selector, redelivery, transacted, false, false, direction);
    }
}
