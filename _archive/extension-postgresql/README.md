# AxonIQ Framework - PostgreSQL Extension

This repository provides an extension to the [Axon Framework](https://github.com/AxonFramework/AxonFramework), for using
it with PostgreSQL. It does so by providing a dedicated Event Storage solution for event-driven and event sourcing
applications, through PostgreSQL. Furthermore, it follows the principles of the Dynamic Consistency Boundary, leading to
greater flexibility for your application evolution.

For more information on anything Axon, please visit our website, [http://axoniq.io](http://axoniq.io).

## Getting started

Prerequisites

* Java 21 or higher
* Axon Framework 5
* PostgreSQL driver version >= 42.6.0

To use this Postgres event storage engine, you can register it as an `EventStorageEngine` 
with the `EventSourcingConfigurer` in your project. Currently, the engine can only 
be configured with a `ConnectionExecutor` which manages execution of JDBC statements 
and an `EventConverter` for converting payloads to a raw byte array suitable for storage:

```java
EventSourcingConfigurer configurer = EventSourcingConfigurer.create()
    .registerEventStorageEngine(componentRegistry ->
        new PostgresqlEventStorageEngine(
            new DataSourceConnectionExecutor(dataSource),
            new DelegatingEventConverter(new JacksonConverter())
        )
    )

/*
 * Register some entities here with configurer.registerEntity, then
 * start the configuration:
 */

AxonConfiguration configuration = configurer.build();

EventStore store = configuration.getComponent(EventStore.class);
CommandGateway commandGateway = configuration.getComponent(CommandGateway.class);
QueryGateway queryGateway = configuration.getComponent(QueryGateway.class);

configuration.start();
```

The constructor will create two tables with (for now) the fixed names `events` and 
`consistency_tags`, with appropriate indices, as well as a sequence called `events_monotonic_seq`.

For any other details concerning Axon, be sure to check [AxonIQ Docs](https://docs.axoniq.io/home/).

## Receiving help

Are you having trouble using the extension?
We'd like to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* Checking the [documentation](https://docs.axoniq.io/home/) should be your first stop,
  as the majority of possible scenarios you might encounter when using Axon should be covered there.
* If the Reference Guide does not cover a specific topic you would've expected,
  we'd appreciate if you could post
  a [new thread/topic on our library forums describing the problem](https://discuss.axoniq.io/c/26).
* There is a [forum](https://discuss.axoniq.io/) to support you in the case the reference guide did not sufficiently
  answer your question.
  AxonIQ's developers will help out on a best effort basis.
  Know that any support from contributors on posted question is very much appreciated on the forum.
* Next to the forum we also monitor Stack Overflow for any questions which are tagged with `axon`.

## Licensing

Axon Framework consists out of a number of different modules, each with different licenses. Modules residing under
the [Axon Framework](https://github.com/AxonFramework) GitHub organization, with group identifier `org.axonframework`,
are Apache 2 licensed. Modules under the [AxonIQ](https://github.com/AxonIQ) GitHub organization, with group identifier
`io.axoniq`, are licensed under AxonIQ's proprietary license.

Please refer to individual module's LICENSE file for details.
