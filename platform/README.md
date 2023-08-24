# AxonIQ Console Framework Client

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.axoniq.console/console-framework-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.axoniq.console/console-framework-client)

AxonIQ Console superpowers your Axon Framework application with advanced monitoring and enabling easy access to actions
within the framework.

![Screenshot of the handler performance screen](.github/img/screenshot_handler_performance.png)

This repository contains the Open-Source connectors that your application will use through maven dependencies.
For actual configuration, please consult the setup instructions that will be provided by AxonIQ Console itself.

[You can visit AxonIQ Console here.](https://console.axoniq.io)

## Spring Boot Starter

### application properties

* `axoniq.console.enabled` - allows disabling the autoconfiguration, default: `true`
* `axoniq.console.dlq-enabled` - allows access to the messages in the DLQ, default: `true`

## Data sent to AxonIQ

AxonIQ Console is an [AxonIQ](https://axoniq.io) SaaS product. Your application will periodically or upon request send
information to the servers of AxonIQ. Please check our [legal documentation](https://console.axoniq.io/legal) for the
measures we implemented to protect your data.

The following data will be sent to the servers of AxonIQ:

- Event processor information
  - Name, latency, status, position
  - Occurs every 2 seconds
- Handler statistics
  - Message names and names of handling components
  - Message payload, or ids, are not sent to AxonIQ servers
  - Statistics such as latency, throughput and error rates
  - Correlation between messages and different handlers
  - Occurs every 20 seconds
- Dead Letter Information
  - Contains message name, error information and event payload
  - Occurs upon user request
  - Can be disabled by 

If you are concerned about the message data being sent to AxonIQ, please contact us at,
disabling the DLQ functionality will prevent that in all cases.