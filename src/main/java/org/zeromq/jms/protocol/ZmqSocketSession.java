package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;

/**
 *  NOTE: THIS IS NOT THREAD SAVE, IT IS NOT MENT TO BE
 *  This class maintains the Zero MQ socket using its own thread. No need for locks, it is
 *  single threaded, where it call out into the multi-threaded gateway.
 */
public class ZmqSocketSession implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ZmqSocketSession.class.getCanonicalName());

    private volatile ZmqSocketStatus status = ZmqSocketStatus.STOPPED;

    private static final int SOCKET_RETRY_MILLI_SECOND = 3000;

    private volatile long lastReceiveTime = System.nanoTime();
    private volatile long lastSendTime    = System.nanoTime();

    private final AtomicBoolean active;
    private final String name;

    private final ZMQ.Socket socket;
    private final ZmqSocketType socketType;
    private final String socketAddr;
    private final boolean socketBound;
    private final boolean socketIncoming;
    private final boolean socketOutgoing;
    private final int socketFlags;
    private final int socketWaitTime;
    private final boolean socketHeartbeat;
    private final boolean socketAcknowledge;
    private final ZmqSocketListener socketListener;

    private final ZmqSocketMetrics metrics;
    private final ZmqEventHandler handler;
    private final ZmqFilterPolicy filter;

    /**
     * Message tacking class.
     */
    public static final class TrackEvent {
        private final ZmqEvent event;
        private final long eventSent;

        /**
         * Construct the event tracker instance.
         * @param event      the event
         * @param eventSent  the event sent time (nano seconds)
         */
        TrackEvent(final ZmqEvent event, final long eventSent) {
            this.event = event;
            this.eventSent = eventSent;
        }

        /**
         * @return  return the event being tracked
         */
        public ZmqEvent getEvent() {
            return event;
        }

        /**
         * @return  return the time the event was sent
         */
        public long getEventSent() {
            return eventSent;
        }

        @Override
        public String toString() {
            return "TrackEvent [eventSent=" + eventSent + ", event=" + event + "]";
        }
    }

    private final Map<Object, TrackEvent> trackEventMap = new HashMap<Object, TrackEvent>();

    /**
     * Socket session constructor.
     * @param name                 the gateway name
     * @param active               the gateway active
     * @param socket               the socket
     * @param socketType           the socket type
     * @param socketAddr           the socket address
     * @param socketBound          the socket "bind" indicator
     * @param socketIncoming       the socket incoming indicator
     * @param socketOutgoing       the socket outgoing indicator
     * @param socketFlags          the socket flags
     * @param socketWaitTime       the socket wait time (milliseconds)
     * @param socketHeartbeat      the socket send "heart-beat" indicator
     * @param socketAcknowledge    the socket always "acknowledge" indicator
     * @param socketListener       the socket listener
     * @param filter               the ZMQ message filter policy
     * @param handler              the message event handler
     * @param metrics              the metrics for the socket
     */
    public ZmqSocketSession(final String name, final AtomicBoolean active,
        final ZMQ.Socket socket, final ZmqSocketType socketType, final String socketAddr, final boolean socketBound,
        final boolean socketIncoming, final boolean socketOutgoing, final int socketFlags, final int socketWaitTime,
        final boolean socketHeartbeat, final boolean socketAcknowledge,
        final ZmqSocketListener socketListener, final ZmqFilterPolicy filter, final ZmqEventHandler handler,
        final ZmqSocketMetrics metrics) {

        this.name = name;
        this.active = active;

        this.socket = socket;
        this.socketType = socketType;
        this.socketAddr = socketAddr;
        this.socketBound = socketBound;
        this.socketIncoming = socketIncoming;
        this.socketOutgoing = socketOutgoing;
        this.socketFlags = socketFlags;
        this.socketWaitTime = socketWaitTime;
        this.socketHeartbeat = socketHeartbeat;
        this.socketAcknowledge = socketAcknowledge;
        this.socketListener = socketListener;

        this.filter = filter;
        this.handler = handler;
        this.metrics = metrics;
    }

    /**
     * @return  return the socket address
     */
    public String getAddr() {
        return socketAddr;
    }

    /**
     * @return  return the socket metrics
     */
    public ZmqSocketMetrics getMetrics() {
        return metrics;
    }

    /**
     * @return  return the socket bound indicator
     */
    public boolean isBound() {
        return socketBound;
    }

    /**
     * @return  return the socket incoming indicator
     */
    public boolean isIncoming() {
        return socketIncoming;
    }

    /**
     * @return  return the socket outgoing indicator
     */
    public boolean isOutgoing() {
        return socketOutgoing;
    }

    /**
     * @return  return the socket send heart-beat indicator
     */
    public boolean isHeartbeat() {
        return socketHeartbeat;
    }

    /**
     * @return  return the socket always acknowledge indicator
     */
    public boolean isAcknowledge() {
        return socketAcknowledge;
    }

    /**
     * @return  return the current socket status
     */
    public ZmqSocketStatus getStatus() {
        return status;
    }

    /**
     * Remove the event from the tracker list.
     * @param  messageId  the message to be removed
     * @return            return null when not found, or the Tacked Event
     */
    public TrackEvent untrack(final Object messageId) {
        return trackEventMap.remove(messageId);
    }

    /**
     * @return  return a list of events that are no-longer being tracked
     */
    public List<TrackEvent> untrackAll() {
        List<TrackEvent> untrackEvents = new ArrayList<TrackEvent>(trackEventMap.values());
        trackEventMap.clear();
        return untrackEvents;
    }

    /**
     * Add the event to the tracker list.
     * @param  event   the event to track
     * @return         return the tracker event
     */
    public TrackEvent track(final ZmqEvent event) {
        final long messageSent = System.currentTimeMillis();
        final Object messageId = event.getMessageId();
        final TrackEvent tackEvent = new TrackEvent(event, messageSent);

        return trackEventMap.put(messageId, tackEvent);
    }

    /**
     * @return  return the total number of events being tracked
     */
    public int trackedCount() {
        return trackEventMap.size();
    }

    /**
     * Return true when the event is being tracked.
     * @param  messageId  the message id of the event
     * @return            return true on tracking
     */
    public boolean isTracked(final Object messageId) {
        return trackEventMap.containsKey(messageId);
    }

    /**
     * Set the socket status.
     * @param status  the new status
     */
    protected void setStatus(final ZmqSocketStatus status) {
        if (this.status != status) {
            this.status = status;
            metrics.setStatus(status);

            LOGGER.log(Level.INFO, "Socket [" + name + "@" + socketAddr + "] changed status: " + status);
        }
    }

    /**
     * Pause the socket.
     */
    public void pause() {
        setStatus(ZmqSocketStatus.PAUSED);

        LOGGER.warning("Socket paused: " + this);
    }

    /**
     * @return  return the last time a message was received (nano seconds)
     */
    public long getLastReceiveTime() {
        return lastReceiveTime;
    }

    /**
     * @return  return the last time a message was sent (nano seconds)
     */
    public long getLastSendTime() {
        return lastSendTime;
    }

    @Override
    public void run() {
        setStatus(ZmqSocketStatus.PENDING);

        // Retry loop is for for the bind, a connection will come successful
        do {
            // Only one of the sockets can be "bind", the others were come back pending
            final ZmqSocketStatus status = openSocket(this);

            if (status == (ZmqSocketStatus.RUNNING) || !active.get()) {
                break;
            }

            // Sleep and retry to bind again
            try {
                Thread.sleep(SOCKET_RETRY_MILLI_SECOND);
            } catch (InterruptedException ex) {
                LOGGER.warning("Opening of socket hibernation interrupted: " + this);
            }
        } while (status == ZmqSocketStatus.PAUSED && active.get());

        if (status == ZmqSocketStatus.RUNNING && active.get()) {
            // Only set a wait for "consumer sockets"
            if (socketOutgoing) {
                socket.setReceiveTimeOut(0);
            } else {
                socket.setReceiveTimeOut(socketWaitTime);
            }

            while (active.get()) {
                if (socketOutgoing) {
                    status = sendSocket(this);

                    if (status == ZmqSocketStatus.ERROR) {
                        break;
                    }
                }

                if (socketIncoming) {
                    status = receiveSocket(this);

                    if (status == ZmqSocketStatus.ERROR) {
                        break;
                    }
                }
            }

            // Check for ACK on last time
            if (socketHeartbeat && socketOutgoing && socketIncoming) {
                socket.setReceiveTimeOut(socketWaitTime);
                sendSocket(this);
            }
        }

        closeSocket(this);
        setStatus(ZmqSocketStatus.STOPPED);
    }

    /**
     * Open and either "bind" or "connect" to the ZMQ socket.
     * @param  socketSession  the session of the socket
     * @return                return the socket status
     */
    protected ZmqSocketStatus openSocket(final ZmqSocketSession socketSession) {
        final String socketAddr = socketSession.socketAddr;
        final ZMQ.Socket socket = socketSession.socket;

        final boolean openSocket = socketListener.open(this);

        if (!openSocket) {
            setStatus(ZmqSocketStatus.PAUSED);
            return getStatus();
        }

        if (socketBound) {
            try {
                socket.bind(socketAddr);
            } catch (ZMQException ex) {
                if (ex.getErrorCode() == 48) {
                    setStatus(ZmqSocketStatus.PAUSED);
                    LOGGER.info("Bind socket UNSUCCESSFUL (Already Bound): " + this);

                    return getStatus();
                }

                LOGGER.log(Level.SEVERE, "Socket binding failure: " + this);
                setStatus(ZmqSocketStatus.ERROR);
                throw ex;
            }

            LOGGER.info("Bind socket successful: " + this);
        } else {
            try {
                socket.connect(socketAddr);
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socket connect failure: " + this, ex);
                setStatus(ZmqSocketStatus.ERROR);
                throw ex;
            }

            LOGGER.info("Connect socket successful: " + this);
        }

        setStatus(ZmqSocketStatus.RUNNING);
        return getStatus();
    }

    /**
     * Close Zero MQ "unbind" or "disconnect" socket functionality.
     * @param  socketSession  the socket session
     * @return                return the socket status
     */
    protected ZmqSocketStatus closeSocket(final ZmqSocketSession socketSession) {
        final String socketAddr = socketSession.socketAddr;
        final ZMQ.Socket socket = socketSession.socket;

        if (socketBound) {
            try {
                socket.setLinger(0);
                socket.unbind(socketAddr);
                socket.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socketing unbind failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Unbind socket successful: " + this);
        } else {
            try {
                socket.setLinger(0);
                socket.disconnect(socketAddr);
                socket.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socket disconnect failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Disconnect socket successful: " + this);
        }

        setStatus(ZmqSocketStatus.STOPPED);
        socketListener.close(this);

        return getStatus();
    }

    /**
     * Produce messages for the outgoing message queue onto the specified socket queue.
     * @param  socketSession  the socket session
     * @return                return the socket status
     */
    protected ZmqSocketStatus sendSocket(final ZmqSocketSession socketSession) {
        ZmqEvent socketEvent = null;

        try {
            // Read from the out going event queue
            if (socketListener != null) {
                do {
                    socketEvent = socketListener.send(this);

                    // NOTE: "socketEvent" can be set to NULL by the listener
                    if (socketEvent != null) {
                        final ZMsg msg = handler.createMsg(socketType, filter, socketEvent);
                        final boolean success = msg.send(socket, true);

                        if (success) {
                            metrics.incrementSend();

                            lastSendTime = System.nanoTime();

                            if (LOGGER.isLoggable(Level.FINEST)) {
                                LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] sent message: " + socketEvent);
                            }
                        } else {
                            // Message are being blocker from being sent, probably use to message buffer full
                            LOGGER.log(Level.WARNING, "Error (" + socket.base().errno() + ") on socket [" + name + "@" + socketAddr
                                + "] and was unable to send message: " + socketEvent
                                + ", try increasing queue capacity (i.e. socket.sndHWM=n)");
                            //LOGGER.log(Level.WARNING, "Socket [" + name + "@" + socketAddr + "] Session sleeping "
                            //    + ", try increasing queue capacity (i.e. socket.sndHWM=n)");
                            //setStatus(ZmqSocketStatus.PAUSED);
                            socketListener.error(this, socketEvent);

                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ex) {
                                LOGGER.log(Level.SEVERE, "Socket [" + name + "@" + socketAddr + "] Session sleeping interuupted", ex);
                            }

                            break;
                        }
                    }
                } while (socketEvent != null && active.get());
            }

        } catch (ZmqException ex) {
            LOGGER.log(Level.SEVERE, "Unable to send message due to internal error: " + this, ex);

            socketListener.error(this, socketEvent);
        }

        return getStatus();
    }

    /**
     * Consume message from the incoming message queue from the specified socket queue.
     * @param  socketSession  the socket session
     * @return                return the socket status
     */
    protected ZmqSocketStatus receiveSocket(final ZmqSocketSession socketSession) {
        if (!active.get()) {
            return getStatus();
        }

        try {
            ZMsg msg = ZMsg.recvMsg(socket, socketFlags);

            while (msg != null) {
                metrics.incrementReceive();
                lastReceiveTime = System.nanoTime();

                try {
                    ZmqEvent event = handler.createEvent(socketType, msg);

                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] recieved message: " + event);
                    }

                    if (event != null && socketListener != null) {
                        setStatus(ZmqSocketStatus.RUNNING);

                        final ZmqEvent replyEvent = socketListener.receive(this, event);

                        // Send back a message when requested
                        if (replyEvent != null) {
                            if (socketIncoming) {
                                final ZMsg replyMsg = handler.createMsg(socketType, filter, replyEvent);

                                replyMsg.send(socket, true);
                                metrics.incrementSend();
                                lastSendTime = System.nanoTime();
                            } else {
                                LOGGER.log(Level.SEVERE, "Socketing has not outgoing state: " + this);
                            }

                            if (LOGGER.isLoggable(Level.FINEST)) {
                                LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] sent response message: " + replyEvent);
                            }
                        }
                    }
                } catch (ZmqException ex) {
                    LOGGER.log(Level.SEVERE, "Socketing incoming failure: " + this, ex);
                }

                msg.destroy();
                msg = ZMsg.recvMsg(socket);
            }
        } catch (org.zeromq.ZMQException ex) {
            setStatus(ZmqSocketStatus.ERROR);
            LOGGER.log(Level.SEVERE, "Socketing incoming failure: " + this, ex);
        }

        return getStatus();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((socketAddr == null) ? 0 : socketAddr.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

       if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        ZmqSocketSession other = (ZmqSocketSession) obj;

        if (socketAddr == null) {
            if (other.socketAddr != null) {
                return false;
            }
        } else if (!socketAddr.equals(other.socketAddr)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ZmqSocketSession [name=" + name
            + ", socketType=" + socketType + ", socketAddr=" + socketAddr + ", socketBound=" + socketBound
            + ", socketIncoming=" + socketIncoming + ", socketOutgoing=" + socketOutgoing + ", socketFlags=" + socketFlags
            + ", socketWaitTime=" + socketWaitTime
            + ", socketHeartbeat=" + socketHeartbeat + ", socketAcknowledge=" + socketAcknowledge + "]";
    }
}
