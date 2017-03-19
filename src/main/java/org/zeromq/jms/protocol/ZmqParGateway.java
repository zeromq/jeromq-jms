package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRedeliveryPolicy;
import org.zeromq.jms.protocol.store.ZmqJournalStore;
import org.zeromq.jms.selector.ZmqMessageSelector;

/**
 * PAR (Positive Acknowledgement with Re-Transmission) Gateway.
 */
@ZmqComponent("par")
@ZmqUriParameter("gateway")
public class ZmqParGateway extends AbstractZmqGateway {

    /**
     * Construct the PAR gateway.
     * @param name          the name of display the gateway
     * @param context       the Zero MQ context
     * @param type          the Zero MQ socket type, i.e. Push, Pull, Router, Dealer, etc...
     * @param isBound       the Zero MQ socket bind/connection indicator
     * @param addr          the Zero MQ socket address(es) is comma separated format
     * @param flags         the Zero MQ socket send flags
     * @param filter        the message filter policy
     * @param handler       the message event adaption functionality
     * @param listener      the listener instance
     * @param store         the (optional) message store
     * @param selector      the (optional) message selection policy
     * @param redelivery    the (optional) message re-delivery policy
     * @param transacted    the transaction indicator
     * @param direction     the direction, i.e. Incoming, Outgoing, etc..
     */
    public ZmqParGateway(final String name, final Context context, final ZmqSocketType type, final boolean isBound, final String addr,
            final int flags, final ZmqFilterPolicy filter, final ZmqEventHandler handler, final ZmqGatewayListener listener,
            final ZmqJournalStore store, final ZmqMessageSelector selector, final ZmqRedeliveryPolicy redelivery,
            final boolean transacted, final Direction direction) {

        super(name, context, getType(type, direction), isBound, addr, flags, filter, handler, listener,
            store, selector, redelivery, transacted, getAcknowledge(direction),
            getHeatbreat(direction), direction);
    }

    /**
     * Return the acknowledge indicator based on the direction of the gateway.
     * @param direction  the direction of the gateway
     * @return           return "true" for a auto-acknowledgement
     */
    protected static boolean getAcknowledge(final Direction direction) {
        return (direction == Direction.INCOMING);
    }

    /**
     * Return the heart-beat indicator based on the direction of the gateway.
     * @param direction  the direction of the gateway
     * @return           return "true" for a heart-beat
     */
    protected static boolean getHeatbreat(final Direction direction) {
        return (direction == Direction.OUTGOING);
    }

    /**
     * Return the type of ZMQ socket given for this direction.
     * @param type       the socket type
     * @param direction  the direction
     * @return           return the socket type
     */
    protected static ZmqSocketType getType(final ZmqSocketType type, final Direction direction) {
        if (direction == Direction.OUTGOING) {
            return ZmqSocketType.DEALER;
        }

        return ZmqSocketType.ROUTER;
    }

    @Override
    protected ZMQ.Socket getSocket(final ZMQ.Context context, final int socketType) {
        final ZMQ.Socket socket = super.getSocket(context, socketType);

        socket.setSendTimeOut(1000);

        return socket;
    }
}
