# JeroMQ JMS

## Introduction

This is JMS 2.0.1 wrapper around ZERO MQ to enable JEE applications to use the ZMQ protocol. The current version uses the JERO MQ 0.4.0 ZMQ, but it should also work with JNI instances.

Core to the wrapper is the Gateway classes that act a publisher or subscriber within JMS to/from ZMQ. A gateway contains the protocol for the interaction with the external communicating instances. It also contains 1 or more ZMQ Sockets, to enable failover, and/or parallel through put.

Each gateway has a primary direction, either outgoing, or incoming. All the examples and test use both directions to test the ZERO MQ functionality.

I have made the wrapper very extensible to use;
- the socket type (PUB, SUB, PULL, PUSH, etc…);
- the adaptor/marshal for JMS messages to/from and external ZMQ format;
- the subscription of messages using ZMQ functionality;
- the JNDI context
- the optional message store for DR
- and more.

The library was aimed to work with Spring and with Tomcat. However, it should work in other JEE servers. For this reason I have implement a JMS URI. Sadly there is no open standard, but it is loosely based a similar functionality in Apache MQ.

```
jms:queue:queue_out?gateway=par&socket.type=DEALER&socket.bind=true&socket.addr=tcp://*:95862&redelivery.retry=3
```

## WIKI

Refer to the WIKI for more details

## Release 2.0

This is a major release, with allot of bug fixes and new functionality
- move the JMS version to 2.0.1 and implement the "simplified" API
- update JeroMQ version 4.0, and general all version dependencies
- extends the URI to enable extends of other URIs (drop duplication)
- switch over gateway.{properties} to socket.{propertes} and adds ALL ZMQ properties (i.e. linger, HWM, etc...) to tweak the underlying ZMQ socket
- adds DR to the gateways, to enable failover
- allow N-N (broker-less) messaging (without PROXY n-1-n)
- adds the ZMQ proxy (with failover) to enable n-1-n setups
- fixes issues around the PAR protocol to enable DR, Failover, etc..
- adds Google Protobuf and JSON message marshaling examples in the tests
- adds Spring annotation based test examples
- adds journal store functionality to enable BCP and loss less messaging

## Examples

- Simple Queue with ZMQ PUSH/PULL

```
jms:queue:queue_1?socket.addr=tcp://*:9728&event=stomp
```

- Simple Topic with ZMQ PUB/SUB

```
jms:topic:topic_1?socket.addr=tcp://*:9711&event=stomp
```

- Proxied N-1-N example 

By specifying a proxy on the receiver queue defintion to enable multiple sender connecting to multiple receivers to enable fan in and out. Only one proxy can bind to the sockets, so any others will staying an PEDING state until the bound proxy drops out.

``` 
jms:queue:sender?socket.addr=tcp://*:9728&event=stomp
jms:queue:receiver?proxy.proxyAddr=tcp://*:9728&socket.addr=tcp://*:9729&socket.bind=false&event=stomp
```

- Enable JOURNALING on a queue

```
jms:queue:queueWithJournal?gateway=par&gateway.socket=tcp://*:9711&event=stomp&journal=file
```

- Queue showing ZMQ socket property setting

```jms:queue:socketTest?socket.addr=tcp://*:9999&socket.type=DEALER&socket.bind=false&redelivery=retry&redelivery.retry=0&socket.bindRetryWaitTime=1000&socket.recieveMsgFlag=10&socket.linger=10000&socket.reconnectIVL=10002&socket.backlog=10003&socket.reconnectIVLMax=10004&socket.maxMsgSize=10004&socket.sndHWM=10005&socket.rcvHWM=10006&socket.affinity=10007&socket.identity=identify&socket.rate=10010&socket.recoveryInterval=10011&socket.reqCorrelate=true&socket.reqRelaxed=true&socket.multicastHops=10010&socket.receiveTimeOut=10011&socket.sendTimeOut=10012&socket.tcpKeepAlive=10020&socket.tcpKeepAliveCount=10021&socket.tcpKeepAliveInterval=10022&socket.tcpKeepAliveIdle=10023&socket.sendBufferSize=10030&socket.receiveBufferSize=10031&socket.routerMandatory=true&socket.xpubVerbose=true&socket.ipv4Only=true&socket.delayAttachOnConnect=true
```

- Topic with ZMQ label filters (alternative to JMS subscription filtering)

```
jms:topic:topic_2?socket.addr=tpc://*:9990&filter=propertyTag&filter.subTags=NASA,APAC&filter.pubPropertyName=Region&event=stomp
```

## Contribution Process

This project uses the [C4 process](http://rfc.zeromq.org/spec:16) for all code changes.

## Licensing

Copyright (c) 2015 Jeremy Miller

Copyright other contributors as noted in the AUTHORS.txt file.

Free use of this software is granted under the terms of the Mozilla Public License Version 2.0 (MPL). If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
