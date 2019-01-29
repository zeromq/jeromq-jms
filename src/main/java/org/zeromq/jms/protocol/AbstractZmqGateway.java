package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRedeliveryPolicy;
import org.zeromq.jms.protocol.store.ZmqJournalEntry;
import org.zeromq.jms.protocol.store.ZmqJournalStore;
import org.zeromq.jms.selector.ZmqMessageSelector;
import org.zeromq.jms.util.Stopwatch;

/**
 * Abstract class that implements the gateway functionality around 1 or more ZERO MQ sockets.
 */
public abstract class AbstractZmqGateway implements ZmqGateway {
    private static final Logger LOGGER = Logger.getLogger(AbstractZmqGateway.class.getCanonicalName());

    private static final int HEARTBEAT_RATE_MILLI_SECOND = 1000;
    private static final int AUTO_PAUSE_IDLE_MILLI_SECOND = 3000;

    private static final int SOCKET_STATUS_TIMEOUT_MILLI_SECOND = 5000;
    private static final int SOCKET_WAIT_MILLI_SECOND = 500;
    private static final int SOCKET_METRIC_BUCKET_COUNT = 360;
    private static final int SOCKET_METRIC_BUCKET_INTERVAL_MILLI_SECOND = 10000;

    private static final int LISTENER_THREAD_POOL = 1;
    private static final int LISTENER_WAIT_MILLI_SECOND = 500;


    private AtomicBoolean active = new AtomicBoolean(false);

    private final String name;

    private final ZmqSocketType type;
    private final boolean bound;
    private final String addr;

    private final int flags;

    private ZMQ.Context context;
    private ZMQ.Context proxyContext;

    private final List<ZmqSocketMetrics> metrics;
    private final Map<String, ZmqSocketSession> socketSessions;

    private ZmqProxySession proxySession = null; //optional proxy

    private final boolean transacted;
    private final boolean acknowledge;
    private final boolean heartbeat;
    private final Direction direction;

    private final Date startDateTime;

    private final ZmqSocketContext socketContext;
    private final ZmqRedeliveryPolicy redelivery;
    private final ZmqFilterPolicy filterPolicy;
    private final ZmqEventHandler eventHandler;
    private final ZmqMessageSelector messageSelector;
    private final ZmqJournalStore journalStore;

    private ZmqGatewayListener listener = null;

    private ExecutorService socketExecutor = null;
    private ExecutorService listenerExecutor = null;
    private ExecutorService proxyExecutor = null;

    private final TransferQueue<ZmqSendEvent> incomingQueue = new LinkedTransferQueue<ZmqSendEvent>();
    private final Queue<ZmqSendEvent> incomingSnapshot = new LinkedList<ZmqSendEvent>();

    private final TransferQueue<ZmqSendEvent> outgoingQueue = new LinkedTransferQueue<ZmqSendEvent>();
    private final Queue<ZmqSendEvent> outgoingSnapshot = new LinkedList<ZmqSendEvent>();

    /**
     * Inner class for publishing external JMS messages.
     */
    private class ListenerThread implements Runnable {

        @Override
        public void run() {
            while (active.get() && listener != null) {
                try {
                    final ZmqMessage message = receive(LISTENER_WAIT_MILLI_SECOND);

                    if (message != null) {
                        listener.onMessage(message);
                    }
                } catch (ZmqException ex) {
                    listener.onException(ex);
                }
            }
        }

    }

    /**
     * Construct abstract gateway.
     * @param name              the name of display the gateway
     * @param socketContext     the socket context for the ZMQ socket
     * @param filter            the message filter policy
     * @param handler           the event handler functionality
     * @param listener          the listener instance
     * @param store             the (optional) message store
     * @param selector          the (optional) message selection policy
     * @param redelivery        the (optional) message re-delivery policy
     * @param transacted        the transaction indicator
     * @param acknowledge       the always acknowledge indicator
     * @param heartbeat         the send heart-beat indicator
     * @param direction         the direction, i.e. Incoming, Outgoing, etc..
     */
    public AbstractZmqGateway(final String name, final ZmqSocketContext socketContext,
        final ZmqFilterPolicy filter, final ZmqEventHandler handler, final ZmqGatewayListener listener,
        final ZmqJournalStore store, final ZmqMessageSelector selector, final ZmqRedeliveryPolicy redelivery,
        final boolean transacted, final boolean acknowledge,
        final boolean heartbeat, final Direction direction) {

        this.name = name;
        this.type = socketContext.getType();
        this.socketContext = new ZmqSocketContext(socketContext);
        this.bound = socketContext.isBindFlag();
        this.addr = socketContext.getAddr();
        this.flags = socketContext.getRecieveMsgFlag();
        this.filterPolicy = filter;
        this.eventHandler = handler;
        this.listener = listener;
        this.journalStore = store;
        this.messageSelector = selector;
        this.redelivery = redelivery;
        this.transacted = transacted;
        this.acknowledge = acknowledge;
        this.heartbeat = heartbeat;
        this.direction = direction;
        this.startDateTime = new Date();

        this.metrics = Collections.synchronizedList(new LinkedList<ZmqSocketMetrics>());
        this.socketSessions = Collections.synchronizedMap(new HashMap<String, ZmqSocketSession>());
    }

    /**
     * @return  return a list of socket addresses
     */
    protected String[] getSocketAddrs() {
        String[] addrs = addr.split(",");
        return addrs;
    }

    /**
     * wait for a status to change and return true, otherwise timeout and return false.
     * @param  millis    the milliseconds to wait before giving up
     * @param  onStatus  the set of status you are waiting for
     * @return           return true when the status have been met
     */
    protected boolean waitOnStatus(long millis, final EnumSet<ZmqSocketStatus> onStatus) {
        final Stopwatch stopwatch = new Stopwatch();

        long waitTime = SOCKET_WAIT_MILLI_SECOND;

        if (millis < 0) {
            millis = SOCKET_STATUS_TIMEOUT_MILLI_SECOND;
        }

        if (millis < waitTime) {
            waitTime = millis / 2;
        }
        boolean success = false;

        do {
            success = true;

            for (ZmqSocketSession socketSession : socketSessions.values()) {
                final ZmqSocketStatus status = socketSession.getStatus();

                if (!onStatus.contains(status)) {
                    success = false;
                    break;
                }
            }

            if (success) {
                break;
            }

            stopwatch.sleep(waitTime);
        } while (stopwatch.before(millis));

        return success;
    }

    @Override
    public void open(final int timeout) {
        if (active.get()) {
            return;
        }

        context = ZMQ.context(socketContext.getIOThreads());

        active.set(true);

        if (journalStore != null) {
            try {
                journalStore.open();
            } catch (ZmqException ex) {
                LOGGER.log(Level.SEVERE, "Unable to journal store: " + journalStore, ex);
                return;
            }
        }

        listenerExecutor = Executors.newFixedThreadPool(LISTENER_THREAD_POOL);

        if (listener != null) {
            ListenerThread listenerThread = new ListenerThread();
            listenerExecutor.execute(listenerThread);
        }

        String[] socketAddrs = getSocketAddrs();
        socketExecutor = Executors.newFixedThreadPool(socketAddrs.length);
        proxyExecutor = (socketContext.isProxy()) ? Executors.newFixedThreadPool(1) : null;

        final boolean socketOutgoing = (direction == Direction.OUTGOING || heartbeat || acknowledge);
        final boolean socketIncoming = (direction == Direction.INCOMING || heartbeat || acknowledge);

        // Setup the ZMQ sockets
        for (String socketAddr : socketAddrs) {
            final ZMQ.Socket socket = getSocket(context, socketContext);

            // Set the filters when they exist
            if (type == ZmqSocketType.SUB) {
                if (filterPolicy == null) {
                    socket.subscribe(ZMQ.SUBSCRIPTION_ALL);
                } else {
                    String[] filters = filterPolicy.getSubscirbeTags();
                    if (filters != null) {
                        for (String filter : filters) {
                            byte[] filterAsBytes = filter.getBytes();
                            socket.subscribe(filterAsBytes);
                        }
                    }
                }
            }

            ZmqSocketSession socketSession = socketSessions.get(socketAddr);
            // re-use socket metrics on a closed socket
            ZmqSocketMetrics socketMetrics = (socketSession != null) ? socketSession.getMetrics() : null;

            if (socketMetrics == null) {
                socketMetrics = new ZmqSocketMetrics(socketAddr, SOCKET_METRIC_BUCKET_COUNT,
                    SOCKET_METRIC_BUCKET_INTERVAL_MILLI_SECOND, socketOutgoing, socketIncoming);
                metrics.add(socketMetrics);
            }

            final ZmqSocketListener socketListener = getSocketListener(socketAddr, socketIncoming, socketOutgoing);

            socketSession = new ZmqSocketSession(name, active,
                socket, type, socketAddr, bound, socketIncoming, socketOutgoing, flags,
                SOCKET_WAIT_MILLI_SECOND, heartbeat, acknowledge, socketListener, filterPolicy, eventHandler, socketMetrics);

            // override closed socket (cannot re-use)
            socketSessions.put(socketAddr, socketSession);
            socketExecutor.execute(socketSession);

            // Make sure only ONE bound session (address) is active on startup
            if (socketSession.isBound()) {
                ZmqSocketStatus status = socketSession.getStatus();

                while (status == ZmqSocketStatus.STOPPED || status == ZmqSocketStatus.PENDING) {
                    try {
                        Thread.sleep(SOCKET_WAIT_MILLI_SECOND);
                    } catch (InterruptedException ex) {
                        LOGGER.warning("Binding sleep interrupted: " + this);
                    }
                    status = socketSession.getStatus();
               }
            }
        }

        //Setup the ZMQ PROXY
        if (socketContext.isProxy()) {
            proxyContext = ZMQ.context(socketContext.getIOThreads());

            final String proxyName = "proxy(" + name + ")";
            final String frontSocketAddr = socketContext.getProxyAddr();
            final ZmqSocketType frontSocketType = (socketContext.getProxyType() == null) ? ZmqSocketType.ROUTER : socketContext.getProxyType();
            final boolean frontSocketBound = true;
            final ZMQ.Socket frontSocket = context.socket(frontSocketType.getType());
            final String backSocketAddr = addr;
            final ZmqSocketType backSocketType = (socketContext.getProxyOutType() == null) ? ZmqSocketType.DEALER : socketContext.getProxyOutType();
            final boolean backSocketBound = true;
            final ZMQ.Socket backSocket =  context.socket(backSocketType.getType());

            proxySession =
                new ZmqProxySession(proxyName, active,
                    frontSocket, frontSocketType, frontSocketAddr, frontSocketBound,
                    backSocket, backSocketType, backSocketAddr, backSocketBound);
            proxyExecutor.execute(proxySession);
        }

        waitOnStatus(timeout,
            EnumSet.of(ZmqSocketStatus.RUNNING, ZmqSocketStatus.PAUSED, ZmqSocketStatus.ERROR));

        LOGGER.info("Gateway openned: " + toString());
    }

    /**
     * Construct a ZMQ socket and initialise default settings.
     * @param context           the Zero MQ context
     * @param socketContext     the Zero MQ socket context
     * @return                  return the constructed and initialised ZMQ socket
     */
    protected ZMQ.Socket getSocket(final ZMQ.Context context, final ZmqSocketContext socketContext) {
        final int socketType = socketContext.getType().getType();
        final ZMQ.Socket socket = context.socket(socketType);

        socket.setSendTimeOut(0);

        final Map<String, Object> valueMap = new HashMap<String, Object>();

        for (Method method : socketContext.getClass().getMethods()) {
            if ((method.getParameterTypes().length == 0) && (method.getReturnType() != null)) {
                final String getterMethodName = method.getName();
                if (getterMethodName.startsWith("get")) {
                    try {
                        final Object value = method.invoke(socketContext);

                        if (value != null) {
                            valueMap.put(getterMethodName.substring(3), value);
                        }
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                        LOGGER.log(Level.WARNING, "Ignoring 'getter' as potential 'setter': " + getterMethodName, ex);
                    }
                }
            }
        }

        for (Method method : socket.getClass().getMethods()) {
            if ((method.getParameterTypes().length == 1) && (method.getReturnType() == null)) {
                final String setterMethodName = method.getName();
                if (setterMethodName.startsWith("set")) {
                    final Object value = valueMap.get(setterMethodName.substring(3));

                    if (value != null) {
                        try {
                            method.invoke(socket, value);
                        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                            LOGGER.log(Level.WARNING, "Ignoring 'setting' of socket context': " + setterMethodName, ex);
                        }
                    }
                }
            }
        }

        return socket;
    }

    /**
     * Return a socket listener for socket session. This is the main "event" based routine that will be extended
     * to add additional functionality.
     * @param socketAddr      the ZMQ address
     * @param socketIncoming  the incoming messages indicator
     * @param socketOutgoing  the outgoing messages indicator
     * @return                return the instance of the listener
     */
    protected ZmqSocketListener getSocketListener(final String socketAddr,
        final boolean socketIncoming, final boolean socketOutgoing) {

        final ZmqSocketListener socketListener = new ZmqSocketListener() {
            @Override
            public boolean open(final ZmqSocketSession session) {
                return socketOpen(session);
            }

            @Override
            public ZmqEvent send(final ZmqSocketSession session) {
                return socketSend(session);
            }

            @Override
            public void error(final ZmqSocketSession session, final ZmqEvent event) {
                socketError(session, event);
            }

            @Override
            public ZmqEvent receive(final ZmqSocketSession session, final ZmqEvent event) {
                return socketReceive(session, event);
            }

            @Override
            public boolean close(final ZmqSocketSession session) {
                return socketClose(session);
            }
        };

        return socketListener;
    }

    @Override
    public boolean isActive() {
        return active.get();
    }

    @Override
    public void close(final int timeout) {
        active.set(false);

        if (proxyContext != null) {
            // need to interrupt the proxy
            proxyContext.close();
        }

        if (acknowledge) {
            // Wait for a period before warning about failed ACKS
            int totalCount = 0;

            for (ZmqSocketSession socketSession : socketSessions.values()) {
                final int sessionCount = socketSession.trackedCount();

                if (sessionCount > 0) {
                    LOGGER.info("Gateway [" + name + "@" + socketSession.getAddr() + "] waiting for acknowledgement message(s): "
                        + sessionCount);
                }

                totalCount = totalCount + sessionCount;
            }

            if (totalCount > 0) {
                try {
                    Thread.sleep(SOCKET_WAIT_MILLI_SECOND);
                } catch (InterruptedException ex) {
                    LOGGER.throwing(AbstractZmqGateway.class.getCanonicalName(), "close()", ex);
                }
            }
        }

        // What for sockets to shut down
        waitOnStatus(timeout, EnumSet.of(ZmqSocketStatus.STOPPED));

        if (journalStore != null) {
            try {
                journalStore.close();
            } catch (ZmqException ex) {
                LOGGER.log(Level.SEVERE, "Gateway [" + name + "] unable to close the journal store: " + journalStore, ex);
            }
        }

        if (listenerExecutor != null) {
            try {
                listenerExecutor.shutdown();
                final boolean success = listenerExecutor.awaitTermination(3, TimeUnit.SECONDS);

                if (!success) {
                    LOGGER.severe("Gateway [" + name + "] listener threads failed to stop: " + toString());
                }
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Gateway [" + name + "] listener threads failed to stop: " + toString(), ex);
            }
        }

        if (proxyExecutor != null) {
            try {
                proxyExecutor.shutdown();
                final boolean success = proxyExecutor.awaitTermination(3, TimeUnit.SECONDS);

                if (!success) {
                    LOGGER.warning("Proxy thread fails to stop, until context terminated (ZMQ issue): " + toString());
                }
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Proxy threads failed to stop: " + toString(), ex);
            }
        }

        if (socketExecutor != null) {
            try {
                socketExecutor.shutdown();
                final boolean success = socketExecutor.awaitTermination(3, TimeUnit.SECONDS);

                if (!success) {
                    LOGGER.severe("Socket threads failed to stop: " + toString());
                }
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Socket threads failed to stop: " + toString(), ex);
            }
        }

        // dump all tracked missing
        if (acknowledge) {
            for (ZmqSocketSession socketSession : socketSessions.values()) {
                List<ZmqSocketSession.TrackEvent> lostEvents = socketSession.untrackAll();
                for (ZmqSocketSession.TrackEvent lostEvent : lostEvents) {
                    // Only send events should be logged as warnings, heart-beats can be ignored.
                    if (lostEvent.getEvent() instanceof ZmqSendEvent) {
                        LOGGER.warning("Gateway [" + name + "] has un-acknowledged message (LOST): " + lostEvent);
                    }
                }
            }
        }

        context.close();
        LOGGER.info("Gateway closed: " + toString());
    }

    /**
     * Socket open event has been triggered. For "connecting" sockets the
     * opening is always granted, but for "bind" we must ensure ONLY one of
     * the sockets is bound, while others stay in pending state.
     * @param source  the socket session
     * @return        return the true to open socket (difference between connect and bind)
     */
    protected boolean socketOpen(final ZmqSocketSession source) {
        if (source.isBound()) {
            for (ZmqSocketSession socketSession : socketSessions.values()) {
                ZmqSocketStatus status = socketSession.getStatus();
                if (status == ZmqSocketStatus.RUNNING) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Socket close event has been triggered.
     * @param source  the socket session
     * @return        return the true to close socket
     */
    protected boolean socketClose(final ZmqSocketSession source) {
        return true;
    }

    /**
     * Socket send event has been triggered.
     * @param source  the socket session
     * @return        return the event to be sent by the session
     */
    protected ZmqEvent socketSend(final ZmqSocketSession source) {
        final String socketAddr = source.getAddr();
        final boolean socketOutgoing = source.isOutgoing();

        ZmqEvent sendEvent = null;

        // Only get real message if the socket session is running.
        if (source.getStatus() == ZmqSocketStatus.RUNNING) {
            if (journalStore != null) {
                try {
                    final ZmqJournalEntry journalEntry = journalStore.read();
                    if (journalEntry != null && (!source.isTracked(journalEntry.getMessageId()))) {
                        sendEvent =
                                eventHandler.createSendEvent(journalEntry.getMessageId(), journalEntry.getMessage());
                    }
                } catch (ZmqException ex) {
                    LOGGER.log(Level.WARNING, "Socket [" + name + "@" + socketAddr + "] failed to read from the journal store", ex);
                }
            }

            if (sendEvent == null) {
                try {
                    sendEvent = outgoingQueue.poll(SOCKET_WAIT_MILLI_SECOND, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    LOGGER.log(Level.WARNING, "Socket [" + name + "@" + socketAddr + "] polling of outgoing queue interrupted", ex);
                }
            }
        }

        // No message(s) so send a heart-beat when required
        if (heartbeat && socketOutgoing && sendEvent == null) {
            // check whether a heart-beat need to be sent since the last message sent
            final long lastReceiveTime = source.getLastReceiveTime();
            final long lastSendTime = source.getLastSendTime();
            final long currentTime = System.nanoTime();
            final long lastReceiveLaspedTime = (currentTime - lastReceiveTime) / 1000000;
            final long lastSendLaspedTime = (currentTime - lastSendTime) / 1000000;
            final ZmqSocketStatus status = source.getStatus();

            if (lastReceiveLaspedTime > HEARTBEAT_RATE_MILLI_SECOND && lastSendLaspedTime > HEARTBEAT_RATE_MILLI_SECOND) {
                if (lastReceiveLaspedTime > AUTO_PAUSE_IDLE_MILLI_SECOND && status == ZmqSocketStatus.RUNNING) {
                    // connection has been dropped, so stop sending messages apart from heart-beats
                    source.pause();

                    List<ZmqSocketSession.TrackEvent> redoEvents = source.untrackAll();
                    for (ZmqSocketSession.TrackEvent redoEvent : redoEvents) {
                        // Only save the send events, heart-beats can be ignored.
                        if (redoEvent.getEvent() instanceof ZmqSendEvent) {
                            socketError(source, redoEvent.getEvent());
                        }
                    }

                } else {
                    sendEvent = eventHandler.createHeartbeatEvent();

                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] send heartbeat: " + sendEvent);
                    }
                }
            }

        }

        if (sendEvent != null && (socketOutgoing && acknowledge)) {
            source.track(sendEvent);

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] tacking event: " + sendEvent);
            }
        }

        if (LOGGER.isLoggable(Level.FINEST) && sendEvent != null) {
            LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] send event: " + sendEvent);
        }

        return sendEvent;
    }

    /**
     * There has been an error relating to the following event.
     * @param source   the socket session having the exception
     * @param event    the event involved
     */
    public void socketError(final ZmqSocketSession source, final ZmqEvent event) {
        if (event instanceof ZmqSendEvent) {
            final ZmqSendEvent sendEvent = (ZmqSendEvent) event;

            try {
                outgoingQueue.put(sendEvent);

                if (LOGGER.isLoggable(Level.FINEST) && sendEvent != null) {
                    LOGGER.log(Level.FINEST, "Socket [" + source.getAddr() + "] send event: " + sendEvent);
                }
            } catch (InterruptedException ex) {
                final String socketAddr = source.getAddr();
                LOGGER.log(Level.SEVERE, "Socket [" + name + "@" + socketAddr + "] was unable to re-send event: " + event, ex);
            }
        }
    }

    /**
     * Return the response event for the socket receiving the specified event
     * There has been an error relating to the following event.
     * @param  source   the socket session which received the event
     * @param  event    the event received
     * @return          return a response event
     */
    public ZmqEvent socketReceive(final ZmqSocketSession source, final ZmqEvent event) {
        final String socketAddr = source.getAddr();

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] consume event: " + event);
        }
        if (event instanceof ZmqSendEvent) {
            try {
                if (journalStore != null) {
                    journalStore.create(event.getMessageId(),  ((ZmqSendEvent) event).getMessage());
                }

                incomingQueue.put((ZmqSendEvent) event);
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Socket [" + name + "@" + socketAddr + "] for gateway " + name
                    + " cannot consume message due to intenral error: " + event, ex);

                return null;
            } catch (ZmqException ex) {
                LOGGER.log(Level.SEVERE, "Socket [" + name + "@" + socketAddr + "] for gateway " + name
                    + " cannot store messahe due to intenral error: " + event, ex);

                return null;
            }
        }

        ZmqEvent replyEvent = null;
        if (event instanceof ZmqHeartbeatEvent) {
            // Heart beat is ALL SENDS
            try {
                if (source.isAcknowledge() && source.isIncoming()) {
                    replyEvent = eventHandler.createAckEvent(event);
                }
            } catch (ZmqException ex) {
                LOGGER.log(Level.SEVERE, "Socket [" + name + "@" + socketAddr + "] received corrupt event: " + event, ex);
            }
        } else if (event instanceof ZmqAckEvent) {
            ZmqAckEvent ackEvent = (ZmqAckEvent) event;
            final Object messageId = ackEvent.getMessageId();
            if (messageId == null) {
                LOGGER.log(Level.SEVERE, "Socket [" + name + "@" + socketAddr + "] received corrupt event: " + event);
            } else {
                LOGGER.log(Level.INFO, "Socket [" + name + "@" + socketAddr + "] received ACK event: " + event);
                final ZmqSocketSession.TrackEvent trackedEvent = source.untrack(messageId);
                if (trackedEvent == null) {
                    LOGGER.log(Level.WARNING, "Socket [" + name + "@" + socketAddr + "] received ACK for untracked event: "
                        + event);
                }
            }
        }
        if (replyEvent != null && LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Socket [" + name + "@" + socketAddr + "] reply event: " + event);
        }

        return replyEvent;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void commit() throws ZmqException {
        if (!active.get()) {
            throw new ZmqException("The gateway has been close: " + toString());
        }

        if (!transacted) {
            throw new ZmqException("No transacion started: " + toString());
        }

        synchronized (outgoingSnapshot) {
            for (ZmqSendEvent event : outgoingSnapshot) {
                outgoingQueue.add(event);

                if (journalStore != null) {
                    journalStore.create(event.getMessageId(), event.getMessage());
                }
            }

            outgoingSnapshot.clear();
        }

        synchronized (incomingSnapshot) {
            if (redelivery != null) {
                redelivery.delivered(incomingSnapshot);

            }

            for (final ZmqSendEvent event : incomingSnapshot) {
                if (journalStore != null) {
                    journalStore.delete(event.getMessageId());
                }
            }

            incomingSnapshot.clear();
        }

        LOGGER.fine("Transaction committed: " + toString());
    }

    @Override
    public void rollback() throws ZmqException {
        if (!active.get()) {
            throw new ZmqException("The gateway has been close: " + toString());
        }

        if (!transacted) {
            throw new ZmqException("No transacion started: " + toString());
        }

        synchronized (outgoingSnapshot) {
            outgoingSnapshot.clear();
        }

        synchronized (incomingSnapshot) {
            if (redelivery != null) {
                redelivery.redeliver(incomingSnapshot);
            }

            incomingSnapshot.clear();
        }

        LOGGER.fine("Transaction rolledback: " + toString());
    }

    @Override
    public void send(final ZmqMessage message) throws ZmqException {
        final ZmqSendEvent event = eventHandler.createSendEvent(message);

        if (transacted) {
            synchronized (outgoingSnapshot) {
                outgoingSnapshot.add(event);
            }
        } else {
            outgoingQueue.add(event);

            if (journalStore != null) {
                journalStore.create(event.getMessageId(), event.getMessage());
            }
        }
    }

    /**
     * Return true when the message passes the JMS selector or non specified.
     * @param message  the message
     * @return         return true on selector pass or no selector specified.
     */
    protected boolean isValidMessage(final ZmqMessage message) {
        if (messageSelector != null) {
            final Map<String, Object> variables = message.getProperties();
            final boolean validMessage = messageSelector.evaluate(variables);

            return validMessage;
        }

        return true;
    }

    @Override
    public ZmqMessage receive() throws ZmqException {
        ZmqMessage message = receive(SOCKET_WAIT_MILLI_SECOND);

        while (message == null && active.get()) {
            message = receive(SOCKET_WAIT_MILLI_SECOND);
        }

        if (message == null) {
            throw new ZmqException("Receive request, buy gateway has been close: " + toString());
        }

        return message;
    }

    @Override
    public ZmqMessage receive(final int timeout) throws ZmqException {
        Stopwatch stopwatch = null;

        if (LOGGER.isLoggable(Level.FINER)) {
            stopwatch = new Stopwatch();
        }

        if (!active.get()) {
            throw new ZmqException("Receive request, buy gateway has been close: " + toString());
        }

        // check for re-delivers
        if (redelivery != null) {
            final ZmqSendEvent event = redelivery.getNextRedeliver();

            if (event != null) {
                final ZmqMessage message = event.getMessage();

                if (transacted) {
                    synchronized (incomingSnapshot) {
                        incomingSnapshot.add(event);
                    }
                }

                if (stopwatch != null) {
                    LOGGER.log(Level.FINER, "Receive re-delivery message: " + stopwatch.lapsedTime() + " (msec) :" + toString());
                }

                return message;
            }
        }

        // check the journal store for any messages.
        if (journalStore != null) {
            ZmqJournalEntry journalEntry = journalStore.read();

            if (journalEntry != null) {
                if (transacted) {
                    final ZmqSendEvent event =
                        eventHandler.createSendEvent(journalEntry.getMessageId(), journalEntry.getMessage());

                    synchronized (outgoingSnapshot) {
                        outgoingSnapshot.add(event);
                    }
                } else {
                    journalStore.delete(journalEntry.getMessageId());
                }

                return journalEntry.getMessage();
            }
        }

        // check the message from the JeroMQ queue.
        final long startTime = System.currentTimeMillis();

        try {
            final ZmqSendEvent event = incomingQueue.poll(timeout, TimeUnit.MILLISECONDS);

            if (event != null) {
                ZmqMessage message = event.getMessage();
                if (isValidMessage(message)) {
                    // when transacted kept track of messages for roll-back
                    if (transacted) {
                        synchronized (incomingSnapshot) {
                            incomingSnapshot.add(event);
                        }
                    } else {
                        if (journalStore != null) {
                            journalStore.create(event.getMessageId(), message);
                        }
                    }

                    if (stopwatch != null) {
                        LOGGER.log(Level.FINER, "Gateway [" + name + "] receive incoming message: " + stopwatch.lapsedTime() + " (msec)");
                    }

                    return message;
                }
            }
        } catch (InterruptedException ex) {
            throw new ZmqException("Unable to poll internal queue: " + toString(), ex);
        }

        final long endTime = System.currentTimeMillis();
        final long lapsedTime = endTime - startTime;

        if (lapsedTime < timeout) {
            final int remainingTimeout = (int) (timeout - lapsedTime);

            final ZmqMessage message = receive(remainingTimeout);

            if (stopwatch != null) {
                if (message == null) {
                    LOGGER.log(Level.FINER, "Gateway  [" + name + "] receive incoming message (Wait): " + stopwatch.lapsedTime() + " (msec)");
                } else {
                    LOGGER.log(Level.FINER, "Gatewau  [" + name + "] receive no message (Timeout): " + stopwatch.lapsedTime() + " (msec)");
                }
            }

            return message;
        }

        return null;
    }

    @Override
    public void setListener(final ZmqGatewayListener listener) {
        if (listener != null && this.listener == null) {
            ListenerThread listenerThread = new ListenerThread();
            listenerExecutor.execute(listenerThread);
        }

        this.listener = listener;
    }

    /**
     * @return  return the metric of the protocol, or null when nothing is being measured.
     */
    public List<ZmqSocketMetrics> getMetrics() {
        return metrics;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getAddr() {
        return addr;
    }

    @Override
    public ZmqSocketType getType() {
        return type;
    }

    @Override
    public ZmqSocketContext getSocketContext() {
        return socketContext;
    }

    @Override
    public boolean isBound() {
        return bound;
    }

    @Override
    public boolean isTransacted() {
        return transacted;
    }

    @Override
    public boolean isAcknowledged() {
        return acknowledge;
    }

    @Override
    public boolean isHeartbeat() {
        return heartbeat;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public Date getStartTime() {
        return startDateTime;
    }

    /**
     * @return  return the ZMQ context
     */
    protected ZMQ.Context getContext() {
        return context;
    }

    /**
     * @return  return the JMS to ZMQ filter resolver
     */
    protected ZmqFilterPolicy getFilterPolicy() {
        return filterPolicy;
    }

    /**
     * @return  return the ZMQ, protocol, JMS message marshaler
     */
    protected ZmqEventHandler getEventHandler() {
        return eventHandler;
    }

    /**
     * @return  return the JMS message selector or null when no selection applied
     */
    protected ZmqMessageSelector getMessageSelector() {
        return messageSelector;
    }

    @Override
    public String toString() {
        return getClass().getCanonicalName()
            + " [active=" + active + ", name=" + name + ", type=" + type + ", isBound=" + bound + ", addr=" + addr
            + ", proxyAddr=" + socketContext.getProxyAddr()
            + ", transacted=" + transacted + ", acknowleged=" + acknowledge + ", heartbeat=" + heartbeat + ", direction=" + direction
            + ", eventHandler=" + eventHandler + ", journalStore=" + journalStore + "]";
    }
}
