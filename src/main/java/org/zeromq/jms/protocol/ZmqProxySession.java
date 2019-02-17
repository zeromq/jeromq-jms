package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import zmq.ZError;

/**
 * This is the session class for the ZMQ PROXY. It has it's own thread, and is not
 * thread safe.
 */
public class ZmqProxySession implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ZmqProxySession.class.getCanonicalName());

    private volatile ZmqSocketStatus status = ZmqSocketStatus.STOPPED;

    private static final int SOCKET_RETRY_MILLI_SECOND = 10000;

    private final AtomicBoolean active;
    private final String name;

    private final ZMQ.Socket frontSocket;
    private final ZmqSocketType frontSocketType;
    private final String frontSocketAddr;
    private final boolean frontSocketBound;

    private final ZMQ.Socket backSocket;
    private final ZmqSocketType backSocketType;
    private final String backSocketAddr;
    private final boolean backSocketBound;

    /**
     * Construct the proxy session instance for the specified sockets.
     * @param name              the name of the proxy
     * @param active            the active status flag of the gateway
     * @param frontSocket       the front socket
     * @param frontSocketType   the front socket type
     * @param frontSocketAddr   the front socket address
     * @param frontSocketBound  the front socket "bind" indicator
     * @param backSocket        the front socket
     * @param backSocketType    the back socket type
     * @param backSocketAddr    the back socket address
     * @param backSocketBound   the back socket "bind" indicator
     */
    public ZmqProxySession(final String name, final AtomicBoolean active,
        final ZMQ.Socket frontSocket, final ZmqSocketType frontSocketType, final String frontSocketAddr, final boolean frontSocketBound,
        final ZMQ.Socket backSocket, final ZmqSocketType backSocketType, final String backSocketAddr, final boolean backSocketBound) {

        this.active = active;
        this.name = name;

        this.frontSocket = frontSocket;
        this.frontSocketType = frontSocketType;
        this.frontSocketAddr = frontSocketAddr;
        this.frontSocketBound = frontSocketBound;

        this.backSocket = backSocket;
        this.backSocketType = backSocketType;
        this.backSocketAddr = backSocketAddr;
        this.backSocketBound = backSocketBound;
    }

    /**
     * Set the socket status.
     * @param status  the new status
     */
    protected void setStatus(final ZmqSocketStatus status) {
        if (this.status != status) {
            this.status = status;

            LOGGER.log(Level.INFO, "Proxy [" + name + "@" + frontSocketAddr + ">"
                + backSocketAddr + "] changed status: " + status);
        }
    }

    @Override
    public void run() {
        setStatus(ZmqSocketStatus.PENDING);

        // NOTE: Open sockets "Back to Front" to ensure message start flowing
        //       only when the backend is active.

        // Retry loop is for for the bind, a connection will come successful
        do {
            // Only one of the sockets can be "bind", the others were come back pending
            final ZmqSocketStatus status = openSocket(backSocket, backSocketAddr, backSocketBound);

            if (status == (ZmqSocketStatus.RUNNING) || !active.get()) {
                break;
            }

            // Sleep and retry to bind again
            try {
                Thread.sleep(SOCKET_RETRY_MILLI_SECOND);
            } catch (InterruptedException ex) {
                LOGGER.warning("Opening of back socket hibernation interrupted: " + this);
            }
        } while (status == ZmqSocketStatus.PAUSED && active.get());

        if (status == ZmqSocketStatus.RUNNING) {
            if (active.get()) {
                do {
                    // Only one of the sockets can be "bind", the others were come back pending
                    final ZmqSocketStatus status = openSocket(frontSocket, frontSocketAddr, frontSocketBound);

                    if (status == (ZmqSocketStatus.RUNNING) || !active.get()) {
                        break;
                    }

                    // Sleep and retry to bind again
                    try {
                        Thread.sleep(SOCKET_RETRY_MILLI_SECOND);
                    } catch (InterruptedException ex) {
                        LOGGER.warning("Opening of front socket hibernation interrupted: " + this);
                    }
                } while (status == ZmqSocketStatus.PAUSED && active.get());

                // Keep running until told to stop
                if (status == ZmqSocketStatus.RUNNING) {
                    if (active.get()) {
                        // Run the proxy until the user interrupts us
                        ZMQ.proxy(frontSocket, backSocket, null);

                        while (active.get()) {
                            // Sleep and retry to bind again
                            try {
                                Thread.sleep(SOCKET_RETRY_MILLI_SECOND);
                            } catch (InterruptedException ex) {
                                LOGGER.warning("Proxy hibernate for failover interrupted: " + this);
                            }
                        }
                    }

                }
            }

        }

        closeSocket(frontSocket, frontSocketAddr, frontSocketBound);
        closeSocket(backSocket, backSocketAddr, backSocketBound);

        setStatus(ZmqSocketStatus.STOPPED);
    }

    /**
     * @return  return the current socket status
     */
    public ZmqSocketStatus getStatus() {
        return status;
    }

    /**
     * Close Zero MQ "unbind" or "disconnect" socket functionality.
     * @param socket          the socket to open
     * @param socketAddr      the address to disconnect/unbind to
     * @param socketBound     the "bind" indicator flag
     * @return                return the socket status
     */
    protected ZmqSocketStatus closeSocket(final ZMQ.Socket socket,
        final String socketAddr, final boolean socketBound) {

        if (socketBound) {
            try {
                try {
                    socket.setLinger(0);
                } catch (ZError.CtxTerminatedException e) {
                    LOGGER.finest("Terminate exception of the setting of linger: " + this);
                }

                try {
                    socket.unbind(socketAddr);
                } catch (ZError.CtxTerminatedException e) {
                    LOGGER.finest("Terminate exception of the unbind: " + this);
                }
                socket.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Proxy [" + socketAddr + "] unbind failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Unbind Proxy [" + socketAddr + "] successful: " + this);
        } else {
            try {
                socket.setLinger(0);
                socket.disconnect(socketAddr);
                socket.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Proxy [" + socketAddr + "] disconnect failure: " + this, ex);
                throw ex;
            }

            LOGGER.info("Disconnect Proxy [" + socketAddr + "] successful: " + this);
        }

        setStatus(ZmqSocketStatus.STOPPED);

        return getStatus();
    }

    /**
     * Open and either "bind" or "connect" to the ZMQ socket.
     * @param socket          the socket to open
     * @param socketAddr      the address to connect/bind to
     * @param socketBound     the "bind" indicator flag
     * @return                return the socket status
     */
    protected ZmqSocketStatus openSocket(final ZMQ.Socket socket,
        final String socketAddr, final boolean socketBound) {

        if (!active.get()) {
            return getStatus();
        }

        if (socketBound) {
            try {
                socket.bind(socketAddr);
            } catch (ZMQException ex) {
                if (ex.getErrorCode() == 48) {
                    setStatus(ZmqSocketStatus.PAUSED);
                    LOGGER.fine("Proxy [" + socketAddr + "] socket UNSUCCESSFUL (Already Bound): " + this);

                    return getStatus();
                }

                LOGGER.log(Level.SEVERE, "Proxy [" + socketAddr + "] binding failure: " + this);
                setStatus(ZmqSocketStatus.ERROR);
                throw ex;
            }

            LOGGER.info("Bind proxy [" + socketAddr + "] successful: " + this);
        } else {
            try {
                socket.connect(socketAddr);
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Proxy [" + socketAddr + "] connect failure: " + this, ex);
                setStatus(ZmqSocketStatus.ERROR);
                throw ex;
            }

            LOGGER.info("Connect Proxy [" + socketAddr + "] successful: " + this);
        }

        setStatus(ZmqSocketStatus.RUNNING);
        return getStatus();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((backSocketAddr == null) ? 0 : backSocketAddr.hashCode());
        result = prime * result + ((frontSocketAddr == null) ? 0 : frontSocketAddr.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        ZmqProxySession other = (ZmqProxySession) obj;
        if (backSocketAddr == null) {
            if (other.backSocketAddr != null) {
                return false;
            }
        } else if (!backSocketAddr.equals(other.backSocketAddr)) {
            return false;
        }
        if (frontSocketAddr == null) {
            if (other.frontSocketAddr != null) {
                return false;
            }
        } else if (!frontSocketAddr.equals(other.frontSocketAddr)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ZmqProxySession [active=" + active + ", name=" + name + ", frontSocketType=" + frontSocketType
                + ", frontSocketAddr=" + frontSocketAddr + ", frontSocketBound=" + frontSocketBound
                + ", backSocketType=" + backSocketType + ", backSocketAddr=" + backSocketAddr + ", backSocketBound="
                + backSocketBound + "]";
    }
}
