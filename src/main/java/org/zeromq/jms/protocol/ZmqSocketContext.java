package org.zeromq.jms.protocol;

import org.zeromq.jms.annotation.ZmqUriParameter;

/**
 * ZMQ socket configuration.
 */
public class ZmqSocketContext {

    private String addr;
    private ZmqSocketType type;
    private Boolean bindFlag;
    private Long bindRetryWaitTime;
    private Integer recieveMsgFlag;
    private String contextName;        // Set to 'addr' when empty
    private int contextIOThreads = 1;  // ZMQ default is suggest as 1

    private String proxyAddr;
    private ZmqSocketType proxyType;
    private ZmqSocketType proxyOutType;

    private Long linger;
    private Long reconnectIVL;
    private Long backlog;
    private Long reconnectIVLMax;
    private Long maxMsgSize;
    private Long sndHWM;
    private Long rcvHWM;
    private Long affinity;

    private byte[] identity;

    private Long rate;
    private Long recoveryInterval;

    private Boolean reqCorrelate;
    private Boolean reqRelaxed;

    private Long multicastHops;
    private Integer receiveTimeOut;
    private Integer sendTimeOut;

    private Long tcpKeepAlive;
    private Long tcpKeepAliveCount;
    private Long tcpKeepAliveInterval;
    private Long tcpKeepAliveIdle;

    private Long sendBufferSize;
    private Long receiveBufferSize;
    private Boolean routerMandatory;
    private Boolean xpubVerbose;
    private Boolean ipv4Only;
    private Boolean delayAttachOnConnect;

    /**
     * Construct empty socket context.
     */
    public ZmqSocketContext() {
    }

    /**
     * Construct socket context with mandatory fields.
     * @param addr            the address
     * @param type            the type of session, i.e. PUSH, PULL, DEALER, etc..
     * @param bindFlag        the flag to specify a bind or connect the socket to the address
     * @param recieveMsgFlag  the message receive flag, i.e. 0 = Do not wait.
     */
    public ZmqSocketContext(final String addr, final ZmqSocketType type, final boolean bindFlag, final int recieveMsgFlag) {

        this.addr = addr;
        this.type = type;
        this.bindFlag = bindFlag;
        this.recieveMsgFlag = recieveMsgFlag;
    }

    /**
     * Copy construct to make the instance immutable.
     * @param context  the context to copy
     */
    public ZmqSocketContext(final ZmqSocketContext context) {
        this.addr = context.addr;
        this.type = context.type;
        this.bindFlag = context.bindFlag;
        this.bindRetryWaitTime = context.bindRetryWaitTime;
        this.recieveMsgFlag = context.recieveMsgFlag;

        this.contextName = context.contextName;
        this.contextIOThreads = context.contextIOThreads;

        this.proxyType = context.proxyType;
        this.proxyOutType = context.proxyOutType;
        this.proxyAddr = context.proxyAddr;

        this.linger = context.linger;
        this.reconnectIVL = context.reconnectIVL;
        this.backlog = context.backlog;
        this.reconnectIVLMax = context.reconnectIVLMax;
        this.maxMsgSize = context.maxMsgSize;
        this.sndHWM = context.sndHWM;
        this.rcvHWM = context.rcvHWM;
        this.affinity = context.affinity;

        this.identity = context.getIdentity();

        this.rate = context.rate;
        this.recoveryInterval = context.recoveryInterval;

        this.reqCorrelate = context.reqCorrelate;
        this.reqRelaxed = context.reqRelaxed;

        this.multicastHops = context.multicastHops;
        this.receiveTimeOut = context.receiveTimeOut;
        this.sendTimeOut = context.sendTimeOut;

        this.tcpKeepAlive = context.tcpKeepAlive;
        this.tcpKeepAliveCount = context.tcpKeepAliveCount;
        this.tcpKeepAliveInterval = context.tcpKeepAliveInterval;
        this.tcpKeepAliveIdle = context.tcpKeepAliveIdle;

        this.sendBufferSize = context.sendBufferSize;
        this.receiveBufferSize = context.receiveBufferSize;
        this.routerMandatory = context.routerMandatory;
        this.xpubVerbose = context.xpubVerbose;
        this.ipv4Only = context.ipv4Only;
        this.delayAttachOnConnect = context.delayAttachOnConnect;
    }

    /**
     * @return  return the address/addresses
     */
    public String getAddr() {
        return addr;
    }

    /**
     * Set the socket address/addresses.
     * @param addr  the address value
     */
    public void setAddr(final String addr) {
        this.addr = addr;
    }

    /**
     * @return  return the socket type, i.e. PULL, PUSH, etc...
     */
    public ZmqSocketType getType() {
        return type;
    }

    /**
     * Change the type of socket, i.e. PULL, PUSH, etc...
     * @param  type   the type of socket
     */
    public void setType(final ZmqSocketType type) {
        this.type = type;
    }

    /**
     * @return  return TRUE when to socket should bind to the address
     */
    public Boolean isBindFlag() {
        return bindFlag != null && bindFlag;
    }

    /**
     * Set the bind/connect flag, "true" is to bind.
     * @param bindFlag  the bind flag value
     */
    public void setBindFlag(final Boolean bindFlag) {
        this.bindFlag = bindFlag;
    }

    /**
     * @return  return unique context name when one exists, otherwise the socket address
     */
    public String getContextName() {
        if (contextName != null && contextName.trim().length() > 0) {
            return contextName;
        }

        return addr;
    }

    /**
     * Set the unique name for the ZMQ context.
     * @param contextName  the unique name for the contract
     */
    @ZmqUriParameter("context.name")
    public void setContextName(final String contextName) {
        this.contextName = contextName;
    }

    /**
     * @return  return context I/O threads
     */
    public int getContextIOThreads() {
        return contextIOThreads;
    }

    /**
     * Set the I/O threads for the ZMQ context (default is 1).
     * @param contextIOThreads  the I/O threads for the contract
     */
    @ZmqUriParameter("context.ioThreads")
    public void setContextIOThreads(final int contextIOThreads) {
        this.contextIOThreads = contextIOThreads;
    }

    /**
     * @return  return true when context has proxy setup details.
     */
    public boolean isProxy() {
        return (proxyAddr != null);
    }

    /**
     * @return  return the proxy address, or NULL when NO PROXY.
     */
    public String getProxyAddr() {
        return proxyAddr;
    }

    /**
     * Set the address for "ZMQ_PROXY" for the socket.
     * @param proxyAddr       the optional proxy address to allow ZMQ_POLLER
     */
    @ZmqUriParameter("proxy.proxyAddr")
    public void setProxyAddr(final String proxyAddr) {
        this.proxyAddr = proxyAddr;
    }

    /**
     * @return  return the proxy socket type.
     */
    public ZmqSocketType getProxyType() {
        return proxyType;
    }

    /**
     * Set the socket type for the "ZMQ_PROXY".
     * @param proxyType       the optional proxy socket type, i.e. ROUTER
     */
    @ZmqUriParameter("proxy.proxyType")
    public void setProxyType(final ZmqSocketType proxyType) {
        this.proxyType = proxyType;
    }

    /**
     * @return  return the proxy outgoing socket type.
     */
    public ZmqSocketType getProxyOutType() {
        return proxyOutType;
    }

    /**
     * Set the outgoing socket type for the "ZMQ_PROXY".
     * @param proxyOutType    the optional proxy socket type, i.e. DEALER
     */
    @ZmqUriParameter("proxy.proxyOutType")
    public void setProxyOutType(final ZmqSocketType proxyOutType) {
        this.proxyOutType = proxyOutType;
    }

    /**
     * @return  Return the wait time (milliseconds) between rebind attempts
     */
    public Long getBindRetryWaitTime() {
        return this.bindRetryWaitTime;
    }

    /**
     * Set the wait time between bind retries.
     * @param bindRetryWaitTime  the wait time in milliseconds
     */
    @ZmqUriParameter("socket.bindRetryWaitTime")
    public void setBindRetryWaitTime(final Long bindRetryWaitTime) {
        this.bindRetryWaitTime = bindRetryWaitTime;
    }

    /**
     * @return  return the message receive flag
     */
    public Integer getRecieveMsgFlag() {
        return recieveMsgFlag;
    }

    /**
     * Set the ZMQ "revcieveMsgFlag", sued on the receive(...., flag) function.
     * @param recieveMsgFlag  the new value, or 0 for no wait.
     */
    @ZmqUriParameter("socket.recieveMsgFlag")
    public void setRecieveMsgFlag(final Integer recieveMsgFlag) {
        this.recieveMsgFlag = recieveMsgFlag;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getLinger() {
        return linger;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  linger  the value to set to
     */
    @ZmqUriParameter("socket.linger")
    public void setLinger(final Long linger) {
        this.linger = linger;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getReconnectIVL() {
        return reconnectIVL;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reconnectIVL  the value to set to
     */
    @ZmqUriParameter("socket.reconnectIVL")
    public void setReconnectIVL(final Long reconnectIVL) {
        this.reconnectIVL = reconnectIVL;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getBacklog() {
        return backlog;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  backlog  the value to set to
     */
    @ZmqUriParameter("socket.backlog")
    public void setBacklog(final Long backlog) {
        this.backlog = backlog;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getReconnectIVLMax() {
        return reconnectIVLMax;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reconnectIVLMax  the value to set to
     */
    @ZmqUriParameter("socket.reconnectIVLMax")
    public void setReconnectIVLMax(final Long reconnectIVLMax) {
        this.reconnectIVLMax = reconnectIVLMax;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getMaxMsgSize() {
        return maxMsgSize;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  maxMsgSize  the value to set to
     */
    @ZmqUriParameter("socket.maxMsgSize")
    public void setMaxMsgSize(final Long maxMsgSize) {
        this.maxMsgSize = maxMsgSize;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getSndHWM() {
        return sndHWM;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  sndHWM  the value to set to
     */
    @ZmqUriParameter("socket.sndHWM")
    public void setSndHWM(final Long sndHWM) {
        this.sndHWM = sndHWM;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getRcvHWM() {
        return rcvHWM;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  rcvHWM  the value to set to
     */
    @ZmqUriParameter("socket.rcvHWM")
    public void setRcvHWM(final Long rcvHWM) {
        this.rcvHWM = rcvHWM;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getAffinity() {
        return affinity;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  affinity  the value to set to
     */
    @ZmqUriParameter("socket.affinity")
    public void setAffinity(final Long affinity) {
        this.affinity = affinity;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public byte[] getIdentity() {
        return identity;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  identity  the value to set to
     */
    @ZmqUriParameter("socket.identity")
    public void setIdentity(final byte[] identity) {
        this.identity = identity;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getRate() {
        return rate;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  rate  the value to set to
     */
    @ZmqUriParameter("socket.rate")
    public void setRate(final Long rate) {
        this.rate = rate;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getRecoveryInterval() {
        return recoveryInterval;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  recoveryInterval  the value to set to
     */
    @ZmqUriParameter("socket.recoveryInterval")
    public void setRecoveryInterval(final Long recoveryInterval) {
        this.recoveryInterval = recoveryInterval;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getReqCorrelate() {
        return reqCorrelate;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reqCorrelate  the value to set to
     */
    @ZmqUriParameter("socket.reqCorrelate")
    public void setReqCorrelate(final Boolean reqCorrelate) {
        this.reqCorrelate = reqCorrelate;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getReqRelaxed() {
        return reqRelaxed;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reqRelaxed  the value to set to
     */
    @ZmqUriParameter("socket.reqRelaxed")
    public void setReqRelaxed(final Boolean reqRelaxed) {
        this.reqRelaxed = reqRelaxed;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getMulticastHops() {
        return multicastHops;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  multicastHops  the value to set to
     */
    @ZmqUriParameter("socket.multicastHops")
    public void setMulticastHops(final Long multicastHops) {
        this.multicastHops = multicastHops;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Integer getReceiveTimeOut() {
        return receiveTimeOut;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  receiveTimeOut  the value to set to
     */
    @ZmqUriParameter("socket.receiveTimeOut")
    public void setReceiveTimeOut(final Integer receiveTimeOut) {
        this.receiveTimeOut = receiveTimeOut;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Integer getSendTimeOut() {
        return sendTimeOut;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  sendTimeOut  the value to set to
     */
    @ZmqUriParameter("socket.sendTimeOut")
    public void setSendTimeOut(final Integer sendTimeOut) {
        this.sendTimeOut = sendTimeOut;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAlive() {
        return tcpKeepAlive;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAlive  the value to set to
     */
    @ZmqUriParameter("socket.tcpKeepAlive")
    public void setTcpKeepAlive(final Long tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAliveCount() {
        return tcpKeepAliveCount;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAliveCount  the value to set to
     */
    @ZmqUriParameter("socket.tcpKeepAliveCount")
    public void setTcpKeepAliveCount(final Long tcpKeepAliveCount) {
        this.tcpKeepAliveCount = tcpKeepAliveCount;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAliveInterval() {
        return tcpKeepAliveInterval;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAliveInterval  the value to set to
     */
    @ZmqUriParameter("socket.tcpKeepAliveInterval")
    public void setTcpKeepAliveInterval(final Long tcpKeepAliveInterval) {
        this.tcpKeepAliveInterval = tcpKeepAliveInterval;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAliveIdle() {
        return tcpKeepAliveIdle;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAliveIdle  the value to set to
     */
    @ZmqUriParameter("socket.tcpKeepAliveIdle")
    public void setTcpKeepAliveIdle(final Long tcpKeepAliveIdle) {
        this.tcpKeepAliveIdle = tcpKeepAliveIdle;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  sendBufferSize  the value to set to
     */
    @ZmqUriParameter("socket.sendBufferSize")
    public void setSendBufferSize(final Long sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  receiveBufferSize  the value to set to
     */
    @ZmqUriParameter("socket.receiveBufferSize")
    public void setReceiveBufferSize(final Long receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getRouterMandatory() {
        return routerMandatory;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  routerMandatory  the value to set to
     */
    @ZmqUriParameter("socket.routerMandatory")
    public void setRouterMandatory(final Boolean routerMandatory) {
        this.routerMandatory = routerMandatory;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getXpubVerbose() {
        return xpubVerbose;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  xpubVerbose  the value to set to
     */
    @ZmqUriParameter("socket.xpubVerbose")
    public void setXpubVerbose(final Boolean xpubVerbose) {
        this.xpubVerbose = xpubVerbose;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getIpv4Only() {
        return ipv4Only;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  ipv4Only  the value to set to
     */
    @ZmqUriParameter("socket.ipv4Only")
    public void setIpv4Only(final Boolean ipv4Only) {
        this.ipv4Only = ipv4Only;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getDelayAttachOnConnect() {
        return delayAttachOnConnect;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  delayAttachOnConnect  the value to set to
     */
    @ZmqUriParameter("socket.delayAttachOnConnect")
    public void setDelayAttachOnConnect(final Boolean delayAttachOnConnect) {
        this.delayAttachOnConnect = delayAttachOnConnect;
    }

    @Override
    public String toString() {
        return "ZmqSocketContext [addr=" + addr + ", type=" + type + ", bindFlag=" + bindFlag
            + ", recieveMsgFlag=" + recieveMsgFlag + ", bindRetryWaitTime=" + bindRetryWaitTime
            + ", proxyAddr= " + proxyAddr + "]";
    }
}
