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
 
## Contribution Process

This project uses the [C4 process](http://rfc.zeromq.org/spec:16) for all code changes.

## Licensing

Copyright (c) 2015 Jeremy Miller

Copyright other contributors as noted in the AUTHORS.txt file.

Free use of this software is granted under the terms of the Mozilla Public License Version 2.0 (MPL). If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
