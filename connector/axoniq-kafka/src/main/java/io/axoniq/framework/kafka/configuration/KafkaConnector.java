/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.kafka.configuration;

import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.LegacyKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.publishing.KafkaEventSink;
import io.axoniq.framework.kafka.publishing.KafkaEventSinkConfiguration;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.streaming.KafkaStreamableEventSource;
import io.axoniq.framework.kafka.streaming.KafkaStreamableEventSourceConfiguration;
import io.axoniq.framework.kafka.subscribing.KafkaSourceStatus;
import io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource;
import io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSourceConfiguration;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentLifecycleHandler;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Configures the connection between an application and Kafka: the sinks that publish events to topics, and the
 * sources that event processors read topics from.
 * <p>
 * Every sink and source is a named component, and names are unique across all sinks and sources. A sink is registered
 * as an {@link EventSink} under its name, and code obtains it with
 * {@code configuration.getComponent(EventSink.class, "<name>")} (in Spring, with an {@code EventSink} parameter
 * qualified with the sink's name). The application's own, unnamed {@code EventSink} is not affected. Processors select
 * a source by name. Nothing is published to Kafka unless a sink is configured and events are published to it, by a
 * handler or through {@link #exposeEvents(String, String, Class[]) exposeEvents}.
 * <pre>{@code
 * MessagingConfigurer.create()
 *     .componentRegistry(cr -> cr.registerEnhancer(
 *             KafkaConnector.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))
 *                           .sink("outbound", TopicResolver.fixed("parcels"),
 *                                 (config, sink) -> sink.withConfirmationMode(ConfirmationMode.TRANSACTIONAL)
 *                                                       .withTransactionalIdPrefix("parcel-service-1-"))
 *                           .streamableSource("parcelStream", TopicSubscription.topics("parcels"))
 *                           .subscribableSource("parcelGroup", TopicSubscription.topics("parcels"), "billing",
 *                                               (config, source) -> source.withConsumerCount(4))
 *     ))
 *     .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(
 *             EventProcessorModule.pooledStreaming("tracking")
 *                                 .eventHandlingComponents(c -> c.autodetected("parcels",
 *                                                                               cfg -> new ParcelProjection()))
 *                                 .customized((cfg, c) -> c.eventSource(
 *                                         cfg.getComponent(StreamableEventSource.class, "parcelStream")))
 *     )));
 * }</pre>
 * Beside the named sinks and sources, the connector registers a {@link KafkaConnectorStatus}, reporting the status of
 * the subscribable sources, and, unless already present:
 * <ul>
 *     <li>a {@link KafkaClientFactory} for the given client configuration;</li>
 *     <li>a {@link KafkaEventCodec}: the {@link DefaultKafkaEventCodec}, converting payloads with the application's
 *     {@link EventConverter} and writing tags with its {@link TagResolver}, when it has one, wrapped in a
 *     {@link LegacyKafkaEventCodec} when {@link #withLegacyFormat()} is set.</li>
 * </ul>
 * When the application configures an
 * {@link io.axoniq.framework.messaging.transformation.events.EventTransformerChain}, the sources apply it to the events
 * they read, just as the event store does.
 * <p>
 * A configuration takes one connector, with all sinks and sources; registering a second one fails with an
 * {@link AxonConfigurationException}. Each sink and source creates its clients through
 * {@link KafkaClientFactory#forComponent(String)}, with its name, so that their client identifiers differ.
 * <p>
 * Sinks close their producers in the {@link Phase#OUTBOUND_EVENT_CONNECTORS} phase. Subscribable sources start and
 * stop consuming in the {@link Phase#INBOUND_EVENT_CONNECTORS} phase. Streamable sources close their clients in the
 * {@link Phase#EXTERNAL_CONNECTIONS} phase.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public final class KafkaConnector implements ConfigurationEnhancer {

    private final @Nullable Map<String, Object> clientConfiguration;
    private boolean legacyFormat;
    private final Map<String, SinkDefinition> sinks = new LinkedHashMap<>();
    private final List<StreamableSourceDefinition> streamableSources = new ArrayList<>();
    private final List<SubscribableSourceDefinition> subscribableSources = new ArrayList<>();
    private final List<ExposureDefinition> exposures = new ArrayList<>();

    private record ExposureDefinition(String processorName,
                                      String sinkName,
                                      Function<Configuration, Set<QualifiedName>> eventNames,
                                      BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
                                              PooledStreamingEventProcessorConfiguration> customization) {

    }

    private record SinkDefinition(
            String name,
            Function<Configuration, TopicResolver> topicResolver,
            BiFunction<Configuration, KafkaEventSinkConfiguration, KafkaEventSinkConfiguration> customization
    ) {

        /**
         * Registers the sink as an {@link EventSink}, so that the framework's event sink decorators, such as tracing,
         * apply. The given {@code adjustment} applies after the customization.
         */
        private void register(ComponentRegistry registry, UnaryOperator<KafkaEventSinkConfiguration> adjustment) {
            Created<KafkaEventSink> created = new Created<>();
            registry.registerComponent(
                    ComponentDefinition.ofTypeAndName(EventSink.class, name)
                                       .withBuilder(config -> created.remember(new KafkaEventSink(
                                               clientFactory(config, name),
                                               adjustment.apply(customization.apply(
                                                       config,
                                                       KafkaEventSinkConfiguration.create(codec(config),
                                                                                          topicResolver.apply(config))
                                               ))
                                       )))
                                       .onShutdown(Phase.OUTBOUND_EVENT_CONNECTORS,
                                                   created.handler(KafkaEventSink::shutdown))
            );
        }
    }

    private record StreamableSourceDefinition(
            String name,
            TopicSubscription subscription,
            BiFunction<Configuration, KafkaStreamableEventSourceConfiguration, KafkaStreamableEventSourceConfiguration>
                    customization
    ) {

        /**
         * Registers the source as processors see it, transformed when the application configures event
         * transformations.
         */
        private void register(ComponentRegistry registry) {
            Created<KafkaStreamableEventSource> created = new Created<>();
            registry.registerComponent(
                    ComponentDefinition.ofTypeAndName(StreamableEventSource.class, name)
                                       .withBuilder(config -> EventTransformationSupport.transforming(
                                               created.remember(new KafkaStreamableEventSource(
                                                       clientFactory(config, name),
                                                       customization.apply(config,
                                                                           KafkaStreamableEventSourceConfiguration
                                                                                   .create(codec(config), subscription))
                                               )),
                                               config
                                       ))
                                       .onShutdown(Phase.EXTERNAL_CONNECTIONS,
                                                   created.handler(KafkaStreamableEventSource::shutdown))
            );
        }
    }

    private record SubscribableSourceDefinition(
            String name,
            TopicSubscription subscription,
            String groupId,
            BiFunction<Configuration, KafkaSubscribableEventSourceConfiguration,
                    KafkaSubscribableEventSourceConfiguration> customization
    ) {

        /**
         * Registers the source as processors see it, transformed when the application configures event
         * transformations, returning a supplier of its status, which reports the Kafka source itself.
         */
        private Supplier<KafkaSourceStatus> register(ComponentRegistry registry) {
            Created<KafkaSubscribableEventSource> created = new Created<>();
            registry.registerComponent(
                    ComponentDefinition.ofTypeAndName(SubscribableEventSource.class, name)
                                       .withBuilder(config -> EventTransformationSupport.transforming(
                                               created.remember(new KafkaSubscribableEventSource(
                                                       clientFactory(config, name),
                                                       customization.apply(config,
                                                                           KafkaSubscribableEventSourceConfiguration
                                                                                   .create(codec(config),
                                                                                           subscription,
                                                                                           groupId)),
                                                       config.getComponent(UnitOfWorkFactory.class)
                                               )),
                                               config
                                       ))
                                       .onStart(Phase.INBOUND_EVENT_CONNECTORS, created.handler(kafkaSource -> {
                                           kafkaSource.start();
                                           return FutureUtils.emptyCompletedFuture();
                                       }))
                                       .onShutdown(Phase.INBOUND_EVENT_CONNECTORS,
                                                   created.handler(KafkaSubscribableEventSource::shutdown))
            );
            return () -> {
                KafkaSubscribableEventSource kafkaSource = created.component;
                return kafkaSource == null ? new KafkaSourceStatus.Stopped() : kafkaSource.status();
            };
        }
    }

    /**
     * The Kafka component behind a registered component, for the lifecycle handlers and status that need the Kafka
     * component itself, while the registered component may decorate it. Empty until the component is built.
     */
    private static final class Created<K> {

        private volatile @Nullable K component;

        private K remember(K created) {
            component = created;
            return created;
        }

        /**
         * Returns a lifecycle handler that applies the given {@code action} to the Kafka component, once built.
         */
        private <C> ComponentLifecycleHandler<C> handler(Function<K, CompletableFuture<?>> action) {
            return (config, registered) -> {
                K current = component;
                return current == null ? FutureUtils.emptyCompletedFuture() : action.apply(current);
            };
        }
    }

    private KafkaConnector(@Nullable Map<String, Object> clientConfiguration) {
        this.clientConfiguration = clientConfiguration == null ? null : Map.copyOf(clientConfiguration);
    }

    /**
     * Creates a connector whose Kafka clients share the given {@code clientConfiguration}, such as the bootstrap
     * servers and security settings.
     *
     * @param clientConfiguration the client configuration shared by all Kafka clients, using the property names of
     *                            the {@code kafka-clients} library
     * @return a connector without sinks and sources
     */
    public static KafkaConnector create(Map<String, Object> clientConfiguration) {
        return new KafkaConnector(Objects.requireNonNull(clientConfiguration,
                                                         "The clientConfiguration must not be null."));
    }

    /**
     * Creates a connector that uses the {@link KafkaClientFactory} component the application registers itself.
     *
     * @return a connector without sinks and sources
     */
    public static KafkaConnector usingRegisteredClientFactory() {
        return new KafkaConnector(null);
    }

    /**
     * Makes the default codec read and write the record headers of Axon Framework 4's Kafka extension as well, for
     * topics shared with applications that still run on Axon Framework 4. See {@link LegacyKafkaEventCodec}.
     *
     * @return this connector, for chaining
     */
    public KafkaConnector withLegacyFormat() {
        this.legacyFormat = true;
        return this;
    }

    /**
     * Configures a sink with the given {@code name}, publishing to the topics the given {@code topicResolver}
     * resolves, with default settings otherwise.
     *
     * @param name          the name of the sink component
     * @param topicResolver the resolver of the topic to publish each event to
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector sink(String name, TopicResolver topicResolver) {
        return sink(name, topicResolver, (config, sink) -> sink);
    }

    /**
     * Configures a sink with the given {@code name}, publishing to the topics the given {@code topicResolver}
     * resolves, with settings adjusted by the given {@code customization}.
     *
     * @param name          the name of the sink component
     * @param topicResolver the resolver of the topic to publish each event to
     * @param customization adjusts the sink's settings, given the application's configuration
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector sink(
            String name,
            TopicResolver topicResolver,
            BiFunction<Configuration, KafkaEventSinkConfiguration, KafkaEventSinkConfiguration> customization
    ) {
        Objects.requireNonNull(topicResolver, "The topicResolver must not be null.");
        return sink(name, config -> topicResolver, customization);
    }

    /**
     * Configures a sink with the given {@code name}, publishing to the topics that the resolver built by the given
     * {@code topicResolverBuilder} resolves, with settings adjusted by the given {@code customization}. The builder
     * runs once, when the sink is created, so that a resolver can come from the application's configuration, such as
     * a component or a bean, without creating it any earlier.
     *
     * @param name                 the name of the sink component
     * @param topicResolverBuilder builds the resolver of the topic to publish each event to, given the application's
     *                             configuration
     * @param customization        adjusts the sink's settings, given the application's configuration
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector sink(
            String name,
            Function<Configuration, TopicResolver> topicResolverBuilder,
            BiFunction<Configuration, KafkaEventSinkConfiguration, KafkaEventSinkConfiguration> customization
    ) {
        requireUnique(name);
        sinks.put(name, new SinkDefinition(
                name,
                Objects.requireNonNull(topicResolverBuilder, "The topicResolverBuilder must not be null."),
                Objects.requireNonNull(customization, "The customization must not be null.")
        ));
        return this;
    }

    /**
     * Configures a source with the given {@code name} for pooled streaming event processors, reading the topics of
     * the given {@code subscription}, with default settings otherwise.
     *
     * @param name         the name of the source component
     * @param subscription the topics to read
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector streamableSource(String name, TopicSubscription subscription) {
        return streamableSource(name, subscription, (config, source) -> source);
    }

    /**
     * Configures a source with the given {@code name} for pooled streaming event processors, reading the topics of
     * the given {@code subscription}, with settings adjusted by the given {@code customization}.
     *
     * @param name          the name of the source component
     * @param subscription  the topics to read
     * @param customization adjusts the source's settings, given the application's configuration
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector streamableSource(
            String name,
            TopicSubscription subscription,
            BiFunction<Configuration, KafkaStreamableEventSourceConfiguration, KafkaStreamableEventSourceConfiguration>
                    customization
    ) {
        requireUnique(name);
        streamableSources.add(new StreamableSourceDefinition(
                name,
                Objects.requireNonNull(subscription, "The subscription must not be null."),
                Objects.requireNonNull(customization, "The customization must not be null.")
        ));
        return this;
    }

    /**
     * Configures a source with the given {@code name} for a subscribing event processor, consuming the topics of the
     * given {@code subscription} in the consumer group with the given {@code groupId}, with default settings
     * otherwise.
     *
     * @param name         the name of the source component
     * @param subscription the topics to consume
     * @param groupId      the consumer group to join
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector subscribableSource(String name, TopicSubscription subscription, String groupId) {
        return subscribableSource(name, subscription, groupId, (config, source) -> source);
    }

    /**
     * Configures a source with the given {@code name} for a subscribing event processor, consuming the topics of the
     * given {@code subscription} in the consumer group with the given {@code groupId}, with settings adjusted by the
     * given {@code customization}.
     *
     * @param name          the name of the source component
     * @param subscription  the topics to consume
     * @param groupId       the consumer group to join
     * @param customization adjusts the source's settings, given the application's configuration
     * @return this connector, for chaining
     * @throws IllegalArgumentException if a sink or source with the given {@code name} is already configured
     */
    public KafkaConnector subscribableSource(
            String name,
            TopicSubscription subscription,
            String groupId,
            BiFunction<Configuration, KafkaSubscribableEventSourceConfiguration,
                    KafkaSubscribableEventSourceConfiguration> customization
    ) {
        requireUnique(name);
        subscribableSources.add(new SubscribableSourceDefinition(
                name,
                Objects.requireNonNull(subscription, "The subscription must not be null."),
                Objects.requireNonNull(groupId, "The groupId must not be null."),
                Objects.requireNonNull(customization, "The customization must not be null.")
        ));
        return this;
    }

    /**
     * Publishes the events with the given payload {@code eventTypes}, as they are, through the sink with the given
     * {@code sinkName}, by a pooled streaming event processor with the given {@code processorName} that starts at the
     * end of the event store. The names of the events are resolved with the application's
     * {@link MessageTypeResolver}, so that the
     * {@link org.axonframework.messaging.eventhandling.annotation.Event @Event} annotation is honored. See
     * {@link #exposeEvents(String, String, BiFunction, Class[])} for the processor, and to adjust its configuration,
     * for example to publish the events stored before as well.
     *
     * @param processorName the name of the processor that publishes the events
     * @param sinkName      the name of the sink to publish through, configured with {@link #sink}
     * @param eventTypes    the payload types of the events to publish
     * @return this connector, for chaining
     * @throws IllegalArgumentException if no event types are given
     */
    public KafkaConnector exposeEvents(String processorName, String sinkName, Class<?>... eventTypes) {
        return exposeEvents(processorName, sinkName, (config, processor) -> processor, eventTypes);
    }

    /**
     * Publishes the events with the given payload {@code eventTypes}, as they are, through the sink with the given
     * {@code sinkName}, by configuring a pooled streaming event processor with the given {@code processorName} that
     * reads the application's event store. The names of the events are resolved with the application's
     * {@link MessageTypeResolver}, so that the
     * {@link org.axonframework.messaging.eventhandling.annotation.Event @Event} annotation is honored. A
     * {@code sinkName} without a configured sink fails with an {@link AxonConfigurationException} when the
     * configuration is built.
     * <p>
     * The processor does not support resets: resetting it would publish every event again.
     * <p>
     * The given {@code customization} receives a configuration whose initial token is the end of the event store when
     * the processor first starts: events stored before then are not published. To publish them as well, set the
     * initial token to the start:
     * <pre>{@code
     * connector.exposeEvents("parcel-exposure", "outbound",
     *                        (config, processor) -> processor.initialToken(source -> source.firstToken(null)),
     *                        ParcelDispatched.class);
     * }</pre>
     * {@link #exposeEvents(String, String, Class[])} keeps the defaults.
     * <p>
     * Exposed events are keyed by the identifier of the aggregate they belong to, when the event store provides one,
     * so that the events of an aggregate land in the same partition. Other exposed events, and the events that
     * handlers publish to the same sink, are keyed by the sink's {@link
     * io.axoniq.framework.kafka.publishing.RecordKeyResolver}.
     * <p>
     * The processor is registered by the connector itself, so the processor settings of Spring Boot's
     * {@code axon.eventhandling.processors} properties do not apply to it; adjust it with the {@code customization}
     * instead.
     *
     * @param processorName the name of the processor that publishes the events
     * @param sinkName      the name of the sink to publish through, configured with {@link #sink}
     * @param customization adjusts the processor's configuration, such as its initial token or segment count
     * @param eventTypes    the payload types of the events to publish
     * @return this connector, for chaining
     * @throws IllegalArgumentException if no event types are given
     */
    public KafkaConnector exposeEvents(
            String processorName,
            String sinkName,
            BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
                    PooledStreamingEventProcessorConfiguration> customization,
            Class<?>... eventTypes
    ) {
        if (Objects.requireNonNull(eventTypes, "The eventTypes must not be null.").length == 0) {
            throw new IllegalArgumentException("At least one event type is required.");
        }
        List<Class<?>> types = List.of(eventTypes);
        return expose(processorName, sinkName, customization, config -> {
            MessageTypeResolver resolver = config.getComponent(MessageTypeResolver.class);
            return types.stream()
                        .map(type -> resolver.resolveOrThrow(type).qualifiedName())
                        .collect(Collectors.toSet());
        });
    }

    /**
     * Publishes the events with the given {@code eventNames}, as they are, through the sink with the given
     * {@code sinkName}, by a pooled streaming event processor with the given {@code processorName} that starts at the
     * end of the event store. Use this variant for events that have no payload class in the application; see
     * {@link #exposeEvents(String, String, BiFunction, Class[])} for the processor, and
     * {@link #exposeEvents(String, String, Set, BiFunction)} to adjust its configuration.
     *
     * @param processorName the name of the processor that publishes the events
     * @param sinkName      the name of the sink to publish through, configured with {@link #sink}
     * @param eventNames    the names of the events to publish
     * @return this connector, for chaining
     * @throws IllegalArgumentException if no event names are given
     */
    public KafkaConnector exposeEvents(String processorName, String sinkName, Set<QualifiedName> eventNames) {
        return exposeEvents(processorName, sinkName, eventNames, (config, processor) -> processor);
    }

    /**
     * Publishes the events with the given {@code eventNames}, as they are, through the sink with the given
     * {@code sinkName}, by a pooled streaming event processor with the given {@code processorName}, whose
     * configuration the given {@code customization} adjusts. Use this variant for events that have no payload class
     * in the application; see {@link #exposeEvents(String, String, BiFunction, Class[])} for the processor and the
     * {@code customization}.
     *
     * @param processorName the name of the processor that publishes the events
     * @param sinkName      the name of the sink to publish through, configured with {@link #sink}
     * @param eventNames    the names of the events to publish
     * @param customization adjusts the processor's configuration, such as its initial token or segment count
     * @return this connector, for chaining
     * @throws IllegalArgumentException if no event names are given
     */
    public KafkaConnector exposeEvents(
            String processorName,
            String sinkName,
            Set<QualifiedName> eventNames,
            BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
                    PooledStreamingEventProcessorConfiguration> customization
    ) {
        if (Objects.requireNonNull(eventNames, "The eventNames must not be null.").isEmpty()) {
            throw new IllegalArgumentException("At least one event name is required.");
        }
        Set<QualifiedName> names = Set.copyOf(eventNames);
        return expose(processorName, sinkName, customization, config -> names);
    }

    private KafkaConnector expose(String processorName,
                                  String sinkName,
                                  BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
                                          PooledStreamingEventProcessorConfiguration> customization,
                                  Function<Configuration, Set<QualifiedName>> eventNames) {
        exposures.add(new ExposureDefinition(
                Objects.requireNonNull(processorName, "The processorName must not be null."),
                Objects.requireNonNull(sinkName, "The sinkName must not be null."),
                eventNames,
                Objects.requireNonNull(customization, "The customization must not be null.")
        ));
        return this;
    }

    private void requireUnique(String name) {
        Objects.requireNonNull(name, "The name must not be null.");
        boolean taken = sinks.containsKey(name)
                || streamableSources.stream().anyMatch(s -> s.name().equals(name))
                || subscribableSources.stream().anyMatch(s -> s.name().equals(name));
        if (taken) {
            throw new IllegalArgumentException("A Kafka sink or source named [" + name + "] is already configured.");
        }
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        if (registry.hasComponent(KafkaConnectorStatus.class)) {
            throw new AxonConfigurationException(
                    "A KafkaConnector is registered already. Register one KafkaConnector per configuration, with all "
                            + "sinks and sources: the sinks and sources of a second connector would silently use the "
                            + "client factory and codec of the first, as connecting to several Kafka clusters is not "
                            + "supported."
            );
        }
        if (clientConfiguration != null) {
            registry.registerIfNotPresent(KafkaClientFactory.class,
                                          config -> KafkaClientFactory.create(clientConfiguration));
        }
        boolean legacy = legacyFormat;
        registry.registerIfNotPresent(KafkaEventCodec.class, config -> defaultCodec(config, legacy));
        Set<String> exposingSinks = exposures.stream().map(ExposureDefinition::sinkName).collect(Collectors.toSet());
        sinks.values().forEach(definition -> definition.register(
                registry,
                exposingSinks.contains(definition.name()) ? KafkaConnector::keyingByAggregate : UnaryOperator.identity()
        ));
        streamableSources.forEach(definition -> definition.register(registry));
        Map<String, Supplier<KafkaSourceStatus>> statuses = new LinkedHashMap<>();
        subscribableSources.forEach(definition -> statuses.put(definition.name(), definition.register(registry)));
        registry.registerComponent(KafkaConnectorStatus.class, config -> new KafkaConnectorStatus(statuses));
        exposures.forEach(exposure -> registerExposure(registry, exposure));
    }

    /**
     * Keys the events that an exposure processor publishes through the sink by the identifier of the aggregate they
     * belong to, and other events with the sink's own record key resolver.
     */
    private static KafkaEventSinkConfiguration keyingByAggregate(KafkaEventSinkConfiguration configuration) {
        return configuration.withRecordKeyResolver(
                EventExposingHandlingComponent.keyingByAggregate(configuration.recordKeyResolver())
        );
    }

    private void registerExposure(ComponentRegistry registry, ExposureDefinition exposure) {
        if (!sinks.containsKey(exposure.sinkName())) {
            throw new AxonConfigurationException("Events are exposed through Kafka sink [" + exposure.sinkName()
                                                         + "], but no sink with that name is configured.");
        }
        registry.registerModule(
                EventProcessorModule.pooledStreaming(exposure.processorName())
                                    .eventHandlingComponents(components -> components.declarative(
                                            exposure.processorName(),
                                            config -> new EventExposingHandlingComponent(
                                                    exposure.processorName(),
                                                    config.getComponent(EventSink.class, exposure.sinkName()),
                                                    exposure.eventNames().apply(config)
                                            )
                                    ))
                                    // Never republish history by accident: start at the end of the event store.
                                    .customized((config, processor) -> exposure.customization().apply(
                                            config,
                                            processor.initialToken(source -> source.latestToken(null))
                                    ))
                                    .build()
        );
    }

    private static KafkaClientFactory clientFactory(Configuration config, String componentName) {
        return config.getComponent(KafkaClientFactory.class).forComponent(componentName);
    }

    private static KafkaEventCodec<?> codec(Configuration config) {
        return config.getComponent(KafkaEventCodec.class);
    }

    private static KafkaEventCodec<?> defaultCodec(Configuration config, boolean legacy) {
        EventConverter converter = config.getComponent(EventConverter.class);
        DefaultKafkaEventCodec codec = new DefaultKafkaEventCodec(converter);
        DefaultKafkaEventCodec tagging =
                config.getOptionalComponent(TagResolver.class).map(codec::withTagResolver).orElse(codec);
        return legacy ? new LegacyKafkaEventCodec(tagging, converter) : tagging;
    }
}
