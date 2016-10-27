# JeroMQ JMS

## Introduction

This is JMS 1.1 wrapper around ZERO MQ to enable JEE applications to use the ZMQ protocol. The current version uses the 0.3.5 ZMQ since I developed the wrapper using JERO, but it should also work with JNI instances.

Core to the wrapper is the Gateway classes that act a publisher or subscriber within JMS to/from ZMQ. A gateway contains the protocol for the interaction with the external communicating instances. It also contains 1 or more Sockets, to enable failure, and parallel through put.

Each gateway has a primary direction, either outgoing, or incoming. All the examples and test use both types to test the ZERO MQ functionality.

I have made the wrapper very extensible to use;
-       the socket type (PUB, SUB, PULL, PUSH, etc…);
-       the adaptor/marshal for JMS messages to/from and external ZMQ format;
-       the subscription of messages using ZMQ functionality;
-       the JNDI context
-       and more.

The library was aimed to work with Spring and with Tomcat. However, it should work in other JEE servers. For this reason I have implement a JMS URI. Sadly there is no open standard, but it is loosely based a similar functionality in Apache MQ.

Why bother? By and large this is true. It only has a limited scope within the JMS world which are much more robust, but for those that do not care to much about transactions, and duplications, but want raw speed it does have some merit, or those that wish to a JavaEE only within there application. JeroMQ-JMS was written to be incorporated in another large application which involves the mash-up of distributed and disparate data to be queried and viewed in real time (zero copy of sorts). By using the JMS API as the messaging standard it allows choice (i.e. RabbitMQ, and combinations there-of, etc…), along with communicating with non JMS applications.


## Design

The JMS API is implemented by classes within the packages starting at “org.zeromq.jms”, were generic JSM “Destination” based class implement both queue and topic interfaces. Also, not all methods have been implemented. Un-implemented functionality will throw the  “java.lang.UnsupportedOperationException” exceptions. Core to the JMS Wrapper is the Gateway interface and Gateway Factory. Implementations of the Gateway interface encapsulate the functionality used by the ZERO MQ JSM Provider and Consumers. Within the current version there are 2 implementation which sub-class from and all-encompassing abstract Gateway implementation. They are the “Fire and Forget” and the “Positive Acknowledgment and Re-transmission” protocols.

Each gateway has 1 or more ZMQ addresses. Each address will instantiates SocketSession which is a contain contains a single ZMQ socket (single threaded as per ZMQ requirements). Each sockets can listen to incoming and outgoing messages. Each incoming messages is converted to an event, which is then processed by the gateway and may end up on the income gateway queue as a JMS messages. Not all events are JMS messages ACK events will be consumed during processing. Like wise outgoing JMS based events are put onto the gateway outgoing queue and sent hopefully will be consumed by one of the socket sessions. 

A gateway is based from the meta data of the JMS destination (queue or topic), which is defined as a URI.

```
	jms:queue:latTestOutQueue?gateway=par&gateway.type=DEALER&gateway.bind=true&gateway.addr=inproc://lat_test2&redelivery.retry=3
```


The URI always start with “jms” then “queue or topic” to determine the type of JMS destination. The next value is the name of the destination for lookup, etc…, then zero or more parameters to tailor the gateway. A general rule of thumb is a parameter name without a period implies a class, and with a period implies attributes settings

Option Name | Default Value | Description
--- | --- | ---
gateway | FireAndForget |  
gateway.type | | The type of ZERO MQ socket. The default value will depend on the JMS destination type, and whether it will be used for a producer or a consumer, i.e topics will be ROUTER or DEALER, and queues are PUB or SUB ZeroMQ socket types
gateway.bind | | The ZeroMQ bind or connection specification.
gateway.addr | | A “semi-colon” separated list of  One or more ZeroMQ addresses, i.e.
redelivery.retry | 3 | Number of reties on delivery failure before assigning the message to Dead Letter functionality.

Using a URI seemed a novel idea at the time, and makes TOMCAT context configuration allot easier. A JMS connection has a collection of URIs which is used by the Gateway Factory to construct Gateways for JMS queues and topics. The Factory takes a URI and constructs the Gateway based on properties of the URI and “default” values. The factory uses the UriParameter annotation on the classes and methods to initialize a gateway. This also allows for the extension of the library with user specific classes (i.e. protocols, and message adaptors etc…). When no URI annotations can be found for a parameter on the URI then an attempt is made against  “getters” and “class” names.

More work needs to be done to add more ZMQ sockets options.

The package root structure is the same as Java ZeroMQ, starting with “org.zeromq.jms”, and from there on specializing by functionality. The two core packages are “org.zeromq.jms” containing the ZeroMQ JMS implmentaions of the JMS API, and “org.zeromq.jms.protocol” containing the Gateway functionality. Because there is no broker, there is a JConcole plug-in to provide some idea of the state of the gateways.

Package Name | Description
--- | ---
org.zeromq.jms | Implementation of the JAVA JMS API 1.1
org.zeromq.jms.annotation | Annotation functionality
org.zeromq.jms.jconsole | Java JConsole plug-in (uses JAVA FX 2, so Java 1.6 or above)
org.zeromq.jms.jmx | MBean functionality, which can be view by the JConsole plugin, or in raw form.
org.zeromq.jms.jndi | The Java naming functionality
org.zeromq.jms.protocol | The ZMQ core functionality based around the Gateway
org.zeromq.jms.protocol.event | Event handlers to map to/from JMS with serial and STOMP implementations
org.zeromq.jms.protocol.filter | Allow ZMQ pub/sub filtering of messages (not JMS selectors)
org.zeromq.jms.protocol.message | Internal message contain (a façade) to enable separation of message from protocol.
org.zeromq.jms.protocol.redelivery | Re-delivery strategy functionality, and simple implementation
org.zeromq.jms.selector | JMS based message selection used for TOPIC filtering (not ZMQ pub/sub filtering).
org.zeromq.jms.stomp | STOMP message builder
org.zeromq.jms.util | Utility class

The library comes with serialized JMS and STOMP out of the box. Anything else would require other dependencies which others may or may not want to implement. To implement a new type you will need to create a new event handler.

Extend the functionality of JeroMQ-JMS is done through the implement one or more of the interfaces (i.e. ZmqGateway, ZmqEventHalandler, ZmqFilterPolicy, etc…). You then need to be able to bind it to the URI. Doing this will require the GatwayFactory access to the class. This can be down by either using “org.zerom.jms” as the start of the package of the class, or adding to the “extensionPackageNames” parameter of the factory constructor (see the ZmqObjectFactor and the Junit tests for inspiration). Even once the new class is accessible you will still need to use “ZmqUriParameter” within you class, or ensure the URIL uses class and methods names along with ZmqUriParameter annotation.

As disucssed already the functionality is based around core functional interfaces. To add a new events and/or strategies into the messaging system you will need to add functionality around the ZmqEventHandler interface. Different filtering of messages then ZmqFilterPolicy functionality would be added. Like wise for messager protocols, etc... Alot of thought and effort (re-writes) went into the interface to enable loose coupling and functional flexibility to allow it to be applied to exising zeromq enviroments.

## Transactions

Are there transactions? Of sorts, but no XA, not very robust. When a transaction is started message are queued on a snapshot and only sent to the socket on a commit. If the process fails, you will lose the messages, so your application(s) need to code this functionality, and self-mend (cope with duplicates, identify missing messages). You should be doing this no matter what.

More work could/should be done here, since the JMS message could be bundled into separate ZMQ frames and sent as one message. This was the case in earlier version, but there was only minor performance improvements and only on certain messages sizes.

Certain more work could be done in this area to make more robust transaction, or for that matter XA transactions. Both will have a cost though.

## Contribution process

This project uses the [C4 process](http://rfc.zeromq.org/spec:16) for all code changes.

## Licensing

Copyright (c) 2015 Jeremy Miller

Copyright other contributors as noted in the AUTHORS.txt file.

Free use of this software is granted under the terms of the Mozilla Public License Version 2.0 (MPL). If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
