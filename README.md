<p align="center">
  <a href="https://www.axoniq.io/products/axon-framework">
    <img src="https://raw.githubusercontent.com/AxonFramework/.github/main/images/AxonFrameworkLogo-2025.png" alt="Axon Framework logo" width="500"/>
  </a>
</p>

<p align="center">
  Build modern event-driven systems with AxonIQ technology.
  <br>
  <a href="https://www.axoniq.io/products/axon-framework"><strong>Product Description »</strong></a>
  <br>
  <br>
  <a href="https://github.com/AxonIQ/code-samples">Code Samples Repo</a>
  ·
  <a href="https://developer.axoniq.io/axon-framework/overview">Technical Overview</a>
  ·
  <a href="https://github.com/AxonIQ/AxoniqFramework/issues">Feature / Bug Request</a>
</p>

# Axoniq Framework

[![Maven Central](https://img.shields.io/maven-central/v/io.axoniq.framework/axoniq-framework-bom)](https://central.sonatype.com/artifact/io.axoniq.framework/axoniq-framework-bom)
[![Build Status](https://github.com/AxonIQ/AxoniqFramework/actions/workflows/main.yml/badge.svg)](https://github.com/AxonIQ/AxoniqFramework/actions/workflows/main.yml)
[![SonarCloud Status](https://sonarcloud.io/api/project_badges/measure?project=AxonIQ_AxoniqFrameworkFramework&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=AxonIQ_AxoniqFramework)

Axoniq Framework builds on top of [Axon Framework](https://github.com/AxonFramework/AxonFramework), providing additional features and enhancements for building evolutionary, event-driven microservice systems based on the principles of Domain-Driven Design (DDD), Command-Query Responsibility Separation (CQRS), and Event Sourcing.

<img src="https://library.axoniq.io/axoniq-console-getting-started/main/ac-monitor-axon-framework-applications/_images/ac-message-dependency-diagram.png" alt="Bootstrap logo">

Where Axon Framework provides the foundational building blocks — such as aggregate design handles, aggregate repositories, command buses, saga design handles, event stores, and query buses — Axoniq Framework extends these with additional capabilities, integrations, and sensible defaults that further simplify development.

The messaging support for commands, events, and queries is at the core of these building blocks.
It is the messaging basics that enable an evolutionary approach towards microservices through the [location transparency](https://en.wikipedia.org/wiki/Location_transparency) they provide.

Axoniq Framework also assists in distributing applications to support scalability or fault tolerance, for example.
The most accessible and quick road forward would be to use [Axon Server](https://developer.axoniq.io/axon-server/overview) to seamlessly adjust message buses to distributed implementations.
Axon Server provides a distributed command bus, event bus, query bus, and an efficient event store implementation for scalable event sourcing.

All this helps to create a well-structured application without worrying about the infrastructure.
Hence, your focus can shift from non-functional requirements to your business functionality.

For more information on anything Axoniq, please visit our website, [http://axoniq.io](http://axoniq.io).

## Getting started

Numerous resources can help you on your journey in using Axoniq Framework.
Let's take a look at some of these:
* The [getting started](https://docs.axoniq.io/axon-framework-5-getting-started/) follows an example domain wherein an Axoniq Framework application is constructed.
* The [reference guide](https://docs.axoniq.io) explains all of the components maintained within Axoniq Framework.
* We have our very own [academy](https://academy.axoniq.io/)!
  The introductory courses are free, followed by more in-depth (paid) courses.
* If the guide doesn't help, our [forum](https://discuss.axoniq.io/) provides a place to ask questions you have during development.

## Receiving help

Are you having trouble using any of our libraries or products?
Know that we want to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* Checking the [reference guide](https://docs.axoniq.io) should be your first stop.
* When the reference guide does not cover your predicament, we would greatly appreciate it if you could file a [documentation issue](https://github.com/AxonIQ/AxoniqFramework/issues) for it.
* Our [forum](https://discuss.axoniq.io/) provides a space to communicate with the Axon community to help you out.
  AxonIQ developers will help you out on a best-effort basis.
  And if you know how to help someone else, we greatly appreciate your contributions!
* We also monitor Stack Overflow for any question tagged with [**axon**](https://stackoverflow.com/questions/tagged/axon).
  Similarly to the forum, Axoniq developers help out on a best-effort basis.

## Feature requests and issue reporting

We use GitHub's [issue tracking system](https://github.com/AxonIQ/AxoniqFramework/issues)) for new feature requests, framework enhancements, and bugs.
Before filing an issue, please verify that it's not already reported by someone else.
Furthermore, make sure you are adding the issue to the correct repository!

When filing bugs:
* A description of your setup and what's happening helps us figure out what the issue might be.
* Do not forget to provide the versions of the Axoniq products you're using, as well as the language and version.
* If possible, share a stack trace.
  Please use Markdown semantics by starting and ending the trace with three backticks (```).

When filing a feature or enhancement:
* Please provide a description of the feature or enhancement at hand.
  Adding why you think this would be beneficial is also a great help to us.
* (Pseudo-)Code snippets showing what it might look like will help us understand your suggestion better.
  Similarly as with bugs, please use Markdown semantics for code snippets, starting and ending with three backticks (```).
* If you have any thoughts on where to plug this into the framework, that would be very helpful too.
* Lastly, we value contributions to the framework highly.
  So please provide a Pull Request as well!

## Update Checker and Anonymous Usage Data Collection

The update checker is included in Axon Framework 5, ensuring the security of the Axoniq
Framework application and its modules and provides useful information to its maintainers.

It does so by retrieving available updates and known vulnerabilities for the Axon modules in use. Furthermore, to
detect updates and vulnerabilities, the checker collects anonymous data about your Axoniq Framework installation. This
data is sent to AxonIQ and includes technical information about your environment.

Please read [this page](https://docs.axoniq.io/axon-framework-update-checker/) of our documentation for more details on
why we collect this information, what you get in return, how to opt out, and why this matters. Please check out
our [Privacy Policy](https://www.axoniq.io/privacy-policy) for any privacy concerns.

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=31ffe27e-667c-48ff-8a14-8029d44dfb66" />

