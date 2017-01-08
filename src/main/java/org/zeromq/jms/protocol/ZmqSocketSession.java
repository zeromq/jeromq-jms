package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;

/**
 *  This class maintains the Zero MQ socket using its own thread.
 */
public class ZmqSocketSession implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ZmqSocketSession.class.getCanonicalName());

    private volatile ZmqSocketStatus status = ZmqSocketStatus.STOPPED;

    private volatile long lastReceiveTime = System.nanoTime();
    private volatile long lastSendTime    = System.nanoTime();

    private final AtomicBoolean process;

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
     * Socket session constructor.
     * @param process            the process
     * @param socket             the socket
     * @param socketType         the socket type
     * @param socketAddr         the socket address
     * @param socketBound        the socket "bind" indicator
     * @param socketIncoming     the socket incoming indicator
     * @param socketOutgoing     the socket outgoing indicator
     * @param socketFlags        the socket flags
     * @param socketWaitTime     the socket wait time (milliseconds)
     * @param socketHeartbeat    the socket send "heart-beat" indicator
     * @param socketAcknowledge  the socket always "acknowledge" indicator
     * @param socketListener     the socket listener
     * @param filter             the ZMQ message filter policy
     * @param handler            the message event handler
     * @param metrics            the metrics for the socket
     */
    public ZmqSocketSession(final AtomicBoolean process, final ZMQ.Socket socket, final ZmqSocketType socketType, final String socketAddr,
            final boolean socketBound, final boolean socketIncoming, final boolean socketOutgoing, final int socketFlags, final int socketWaitTime,
            final boolean socketHeartbeat, final boolean socketAcknowledge,
            final ZmqSocketListener socketListener, final ZmqFilterPolicy filter, final ZmqEventHandler handler,
            final ZmqSocketMetrics metrics) {

        this.process = process;

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
     * Pause the socket.
     */
    public void pause() {
        if (status == ZmqSocketStatus.RUNNING) {
            status = ZmqSocketStatus.WAITING;

            LOGGER.warning("Socket paused: " + this);
        }
    }

    /**
     * @return  reutrn the last time a message was received (nano seconds)
     */
    public long getLastReceiveTime() {
        return lastReceiveTime;
    }

    /**
     * @return  reutrn the last time a message was sent (nano seconds)
     */
    public long getLastSendTime() {
        return lastSendTime;
    }

    @Override
    public void run() {
        openSocket(this);

        // Only set a wait for "consumer sockets"
        if (socketOutgoing) {
            socket.setReceiveTimeOut(0);
        } else {
            socket.setReceiveTimeOut(socketWaitTime);
        }

        LOGGER.info("Started socket: " + this);

        while (process.get()) {
            if (socketOutgoing) {
                sendSocket(this);
            }

            if (socketIncoming) {
                receiveSocket(this);
            }

            metrics.setStatus(status);
        }

        LOGGER.info("Stopped socket: " + this);

        // Check for ACK on last time
        if (socketHeartbeat && socketOutgoing && socketIncoming) {
            socket.setReceiveTimeOut(socketWaitTime);
            sendSocket(this);
        }

        closeSocket(this);
    }

    /**
     * Open and either "bind" or "connect" to the ZMQ socket.
     * @param socketSession  the session of the socket
     */
    protected void openSocket(final ZmqSocketSession socketSession) {
        final String socketAddr = socketSession.socketAddr;
        final ZMQ.Socket socket = socketSession.socket;

        if (socketBound) {
            try {
                socket.bind(socketAddr);
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socket binding failure: " + this);
                throw ex;
            }

            LOGGER.info("Bind socket successful: " + this);
        } else {
            try {
                socket.connect(socketAddr);
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socket connect failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Connect socket successful: " + this);
        }

        socketListener.open(this);

        status = ZmqSocketStatus.RUNNING;
    }

    /**
     * Close Zero MQ "unbind" or "disconnect" socket functionality.
     * @param socketSession  the socket session
     */
    protected void closeSocket(final ZmqSocketSession socketSession) {
        final String socketAddr = socketSession.socketAddr;
        final ZMQ.Socket socket = socketSession.socket;

        if (socketBound) {
            try {
                socket.unbind(socketAddr);
                // socket.setLinger(0);
                socket.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socketing unbind failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Unbind socket successful: " + this);
        } else {
            try {
                // socket.setLinger(0);
                socket.disconnect(socketAddr);
                socket.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Socket disconnect failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Disconnect socket successful: " + this);
        }

        socketListener.close(this);

        status = ZmqSocketStatus.STOPPED;
    }

    /**
     * Produce messages for the outgoing message queue onto the specified socket queue.
     * @param  socketSession  the socket session
     */
    protected void sendSocket(final ZmqSocketSession socketSession) {
        final boolean active = (status == ZmqSocketStatus.RUNNING);

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
                        } else {
                            LOGGER.log(Level.SEVERE, "Unable to send message: " + this);

                            status = ZmqSocketStatus.WAITING;
                            socketListener.error(this, socketEvent);

                            break;
                        }
                    }
                } while (socketEvent != null && active);
            }

        } catch (ZmqException ex) {
            LOGGER.log(Level.SEVERE, "Unable to send message due to internal error: " + this, ex);

            socketListener.error(this, socketEvent);
        }
    }

    /**
     * Consume message from the incoming message queue from the specified socket queue.
     * @param  socketSession  the socket session
     */
    protected void receiveSocket(final ZmqSocketSession socketSession) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Receive and wait (" + socket.getReceiveTimeOut() + ") : " + this);
        }

        ZMsg msg = ZMsg.recvMsg(socket, socketFlags);

        while (msg != null) {
            metrics.incrementReceive();
            lastReceiveTime = System.nanoTime();

            try {
                ZmqEvent event = handler.createEvent(socketType, msg);

                if (event != null && socketListener != null) {
                    final ZmqEvent replyEvent = socketListener.receive(this, event);

                    // Send back a message when requested
                    if (replyEvent != null) {
                        if (socketOutgoing) {
                            final ZMsg replyMsg = handler.createMsg(socketType, filter, replyEvent);

                            replyMsg.send(socket, true);

                            metrics.incrementSend();
                            lastSendTime = System.nanoTime();
                        } else {
                            LOGGER.log(Level.SEVERE, "Socketing has not outgoing state: " + this);
                        }
                    }
                }
            } catch (ZmqException ex) {
                LOGGER.log(Level.SEVERE, "Socketing incoming failure: " + this, ex);
            }

            msg.destroy();
            msg = ZMsg.recvMsg(socket);
        }
    }

    @Override
    public String toString() {
        return "ZmqSocketSession [socketType=" + socketType + ", socketAddr=" + socketAddr + ", socketBound=" + socketBound + ", socketIncoming="
                + socketIncoming + ", socketOutgoing=" + socketOutgoing + ", socketFlags=" + socketFlags + ", socketWaitTime=" + socketWaitTime
                + ", socketHeartbeat=" + socketHeartbeat + ", socketAcknowledge=" + socketAcknowledge + "]";
    }
}
