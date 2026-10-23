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

package io.axoniq.framework.kafka.subscribing;

import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.common.ConsumerSettings;
import io.axoniq.framework.kafka.common.KafkaAxoniqAddon;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.license.entitlement.EntitlementManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

/**
 * A {@link SubscribableEventSource} that consumes events from Kafka topics in a consumer group, for use by a
 * subscribing event processor.
 * <p>
 * Kafka divides the work: it assigns the partitions of the subscribed topics to the members of the consumer group,
 * and moves them when members join or leave. Events of a partition are processed in order, and partitions are
 * processed concurrently, so the parallelism is the number of partitions, spread over all consumers of all
 * application instances. Progress is kept as committed offsets in Kafka: the offset after a record is committed only
 * once its event has been processed. A crash before the commit processes the event again, so events are delivered at
 * least once. Axon's sequencing policies, segments, token stores and processor resets do not apply.
 * <p>
 * Every event is processed in a unit of work of its own, whose context carries the
 * {@link io.axoniq.framework.kafka.common.KafkaResources} of the record it came from. Failed events are dealt with by
 * the source itself, according to the configured {@link FailedRecordPolicy}. Events are processed on virtual threads,
 * unless an executor is configured with
 * {@link KafkaSubscribableEventSourceConfiguration#withExecutor(java.util.concurrent.Executor)}.
 * <p>
 * A topic pattern never matches the source's own dead-letter topics; see
 * {@link io.axoniq.framework.kafka.common.TopicSubscription#pattern(java.util.regex.Pattern)}.
 * <p>
 * A source belongs to one subscriber: messages flow only while that subscriber is subscribed, and the consumer
 * group's progress is that subscriber's progress. A second subscriber is rejected with an
 * {@link IllegalStateException}: give each processor a source, with a consumer group, of its own.
 * <p>
 * Consumers start when the source is {@link #start() started} and a subscriber is present, and leave the consumer
 * group when the subscriber unsubscribes or the source {@link #shutdown() shuts down}.
 * <p>
 * Consumers retry transient failures, such as an unreachable broker, for as long as they run. A failure that retrying
 * cannot resolve, such as invalid settings or failed authentication or authorization, stops all consumers of the
 * source and is reported through its {@link #status() status}.
 * <p>
 * The source reads with {@code isolation.level=read_committed}: records of aborted Kafka transactions are skipped,
 * and consumption stops before the records of open ones. An {@code isolation.level} in the source's own
 * {@link KafkaSubscribableEventSourceConfiguration#withConsumerProperties(Map) consumer properties} changes this; one
 * in the settings of the {@link KafkaClientFactory} does not.
 * <p>
 * A new consumer group starts at the earliest record of each partition, unless configured to start at their end with
 * {@link KafkaSubscribableEventSourceConfiguration#withInitialPosition(InitialPosition)}. A committed offset that
 * Kafka has already deleted under the topic's retention settings is dealt with according to the configured
 * {@link io.axoniq.framework.kafka.common.RetentionGapPolicy}; by default, the source stops consuming that partition
 * until the consumer group's offsets are reset.
 * <p>
 * Example usage:
 * <pre>{@code
 * KafkaSubscribableEventSource source = new KafkaSubscribableEventSource(
 *         KafkaClientFactory.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")),
 *         KafkaSubscribableEventSourceConfiguration.create(new DefaultKafkaEventCodec(eventConverter),
 *                                                          TopicSubscription.topics("parcels"),
 *                                                          "billing")
 *                                                  .withConsumerCount(4),
 *         unitOfWorkFactory
 * );
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class KafkaSubscribableEventSource implements SubscribableEventSource, DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSubscribableEventSource.class);

    private final KafkaClientFactory clientFactory;
    private final KafkaSubscribableEventSourceConfiguration configuration;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final EntitlementManager entitlementManager;
    private @Nullable BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>
            subscriber;
    private @Nullable Members<?> members;
    private @Nullable Throwable failure;
    private CompletableFuture<Void> stopped = FutureUtils.emptyCompletedFuture();
    private boolean started;

    /**
     * Constructs a source that consumes events as the given {@code configuration} describes, with clients created by
     * the given {@code clientFactory}.
     *
     * @param clientFactory     the factory creating the Kafka clients of this source
     * @param configuration     the settings of this source
     * @param unitOfWorkFactory the factory of the unit of work each event is processed in
     */
    public KafkaSubscribableEventSource(KafkaClientFactory clientFactory,
                                        KafkaSubscribableEventSourceConfiguration configuration,
                                        UnitOfWorkFactory unitOfWorkFactory) {
        this(clientFactory, configuration, unitOfWorkFactory, EntitlementManager.INSTANCE);
        EntitlementManager.INSTANCE.registerAddon(KafkaAxoniqAddon.class);
    }

    /**
     * Constructs a source that claims consumed events against the given {@code entitlementManager}.
     *
     * @param clientFactory      the factory creating the Kafka clients of this source
     * @param configuration      the settings of this source
     * @param unitOfWorkFactory  the factory of the unit of work each event is processed in
     * @param entitlementManager the entitlement manager consumed events are claimed against
     */
    KafkaSubscribableEventSource(KafkaClientFactory clientFactory,
                                 KafkaSubscribableEventSourceConfiguration configuration,
                                 UnitOfWorkFactory unitOfWorkFactory,
                                 EntitlementManager entitlementManager) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "The clientFactory must not be null.");
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null.");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "The unitOfWorkFactory must not be null.");
        this.entitlementManager = Objects.requireNonNull(entitlementManager,
                                                         "The entitlementManager must not be null.");
        warnAboutIgnoredSettings(configuration);
    }

    private static void warnAboutIgnoredSettings(KafkaSubscribableEventSourceConfiguration configuration) {
        ConsumerSettings.warnAboutIgnoredGroupSettings(configuration.consumerProperties(),
                                                       configuration.groupId(),
                                                       logger);
        if (configuration.subscription() instanceof TopicSubscription.TopicPattern(Pattern pattern)
                && endsInSuffix(pattern.pattern(), configuration.deadLetterTopicSuffix())) {
            logger.warn("The topic pattern [{}] of the Kafka source for consumer group [{}] only matches topics whose "
                                + "names end in the dead-letter topic suffix [{}], which the source leaves out, so it "
                                + "consumes nothing. To consume dead-letter topics, give the source a different "
                                + "dead-letter topic suffix.",
                        pattern.pattern(), configuration.groupId(), configuration.deadLetterTopicSuffix());
        }
    }

    /**
     * Returns whether the given regular expression ends in the given literal {@code suffix}, as written plainly, with
     * its special characters escaped, or quoted, optionally followed by an end anchor.
     */
    private static boolean endsInSuffix(String regex, String suffix) {
        String expression = regex.endsWith("$") ? regex.substring(0, regex.length() - 1) : regex;
        String escaped = suffix.replaceAll("[\\\\.\\[\\]{}()<>*+\\-=!?^$|]", "\\\\$0");
        return expression.endsWith(escaped) || expression.endsWith(Pattern.quote(suffix));
    }

    /**
     * Subscribes the given {@code eventsBatchConsumer} as the one subscriber of this source. Subscribing the same
     * subscriber again has no further effect. Consumers start once the source is also {@link #start() started}.
     *
     * @param eventsBatchConsumer the subscriber that processes the events of this source
     * @return a registration that unsubscribes the subscriber, stopping the consumers
     * @throws IllegalStateException if another subscriber is already subscribed
     */
    @Override
    public synchronized Registration subscribe(
            BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>
                    eventsBatchConsumer
    ) {
        Objects.requireNonNull(eventsBatchConsumer, "The eventsBatchConsumer must not be null.");
        if (subscriber != null && subscriber != eventsBatchConsumer) {
            throw new IllegalStateException(
                    "The Kafka event source for consumer group [" + configuration.groupId() + "] already has a "
                            + "subscriber. Configure a source, with a consumer group of its own, for each processor."
            );
        }
        if (subscriber == null) {
            subscriber = eventsBatchConsumer;
            if (started) {
                startMembers();
            }
        }
        return () -> unsubscribe(eventsBatchConsumer);
    }

    private synchronized boolean unsubscribe(
            BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>> consumer
    ) {
        if (subscriber != consumer) {
            return false;
        }
        stopMembers();
        subscriber = null;
        return true;
    }

    /**
     * Starts consuming, once a subscriber is present. Applications configure the source to start in the
     * {@link org.axonframework.common.lifecycle.Phase#INBOUND_EVENT_CONNECTORS} phase, after subscribing processors
     * have subscribed.
     */
    public synchronized void start() {
        started = true;
        if (subscriber != null && members == null) {
            startMembers();
        }
    }

    /**
     * Stops consuming, leaving the consumer group, and closes the source's clients. All consumers are asked to stop
     * at once. Events in progress are given the configured revocation timeout to
     * finish; their offsets are committed when they do. Events that do not finish in time are delivered again to the
     * next owner of their partition.
     * <p>
     * Stopping takes at most the revocation timeout plus 15 seconds, provided the dead-letter producer's
     * {@code max.block.ms} (10 seconds by default) does not exceed the revocation timeout plus 10 seconds. Give the
     * {@link org.axonframework.common.lifecycle.Phase#INBOUND_EVENT_CONNECTORS} phase at least that long: in Spring
     * Boot with {@code spring.lifecycle.timeout-per-shutdown-phase} (30 seconds by default), and otherwise with
     * {@link org.axonframework.common.configuration.LifecycleRegistry#registerLifecyclePhaseTimeout(long,
     * java.util.concurrent.TimeUnit)} (5 seconds by default).
     *
     * @return a future that completes when the source's consumers have stopped, or have been given up on after the
     * revocation timeout plus the time to commit their offsets and leave the group
     */
    public synchronized CompletableFuture<Void> shutdown() {
        started = false;
        stopMembers();
        return stopped;
    }

    /**
     * Returns the current status of this source: {@link KafkaSourceStatus.Running} while its consumers run,
     * {@link KafkaSourceStatus.Stopped} while they do not, and {@link KafkaSourceStatus.Failed} once a consumer failed
     * in a way that retrying cannot resolve, such as invalid settings or failed authentication or authorization.
     * While the consumers run, the status lists the partitions they own but do not progress on, see
     * {@link KafkaSourceStatus.Running#stoppedPartitions()}.
     * <p>
     * Such a failure is logged once. See {@link KafkaSourceStatus} for when each status applies.
     *
     * @return the current status of this source
     */
    public synchronized KafkaSourceStatus status() {
        if (failure != null) {
            return new KafkaSourceStatus.Failed(failure);
        }
        return members != null
                ? new KafkaSourceStatus.Running(members.stoppedPartitions())
                : new KafkaSourceStatus.Stopped();
    }

    private void startMembers() {
        failure = null;
        Members<?> starting = new Members<>(configuration.codec());
        try {
            starting.start(Objects.requireNonNull(subscriber));
        } catch (RuntimeException | Error e) {
            // Leaves nothing running: stops the consumers started so far, and closes the executor and writer.
            try {
                stopped = CompletableFuture.allOf(stopped, starting.stop());
            } catch (RuntimeException | Error stopFailure) {
                e.addSuppressed(stopFailure);
            }
            throw e;
        }
        members = starting;
    }

    /**
     * Stops all consumers of the given {@code failed} members once one of them failed in a way that retrying cannot
     * resolve, so that the consumer group does not run partially, which is the hardest state to notice.
     */
    private synchronized void stopAfterFailure(Members<?> failed, Throwable error) {
        if (members != failed) {
            // These members were stopped already, for example by a shutdown.
            return;
        }
        failure = error;
        logger.error("A consumer of consumer group [{}] failed in a way that retrying cannot resolve. All consumers of "
                             + "the source stop.", configuration.groupId());
        stopMembers();
    }

    private void stopMembers() {
        Members<?> current = members;
        members = null;
        if (current != null) {
            stopped = CompletableFuture.allOf(stopped, current.stop());
        }
    }

    /**
     * The consumers started for one subscription, with the executor that processes their records and the writer of
     * their dead-letter records. An executor the application provides is not shut down.
     */
    private final class Members<V> {

        private final List<GroupConsumer<V>> members = new ArrayList<>();
        private final KafkaEventCodec<V> codec;
        private final Executor executor;
        // The executor the source creates itself when the application provides none; the source shuts it down.
        private final @Nullable ExecutorService ownedExecutor;
        private final DeadLetterTopicWriter<V> deadLetterTopicWriter;

        /**
         * Creates the writer of the dead-letter records and, when the application provides none, the executor that
         * processes the records. Consumers are created and started by {@link #start(BiFunction)}.
         */
        private Members(KafkaEventCodec<V> codec) {
            this.codec = codec;
            this.deadLetterTopicWriter = DeadLetterTopicWriter.create(clientFactory,
                                                                      configuration.producerProperties(),
                                                                      codec.valueSerde().serializer(),
                                                                      configuration.deadLetterTopicSuffix());
            this.ownedExecutor = configuration.executor().isEmpty()
                    ? Executors.newVirtualThreadPerTaskExecutor()
                    : null;
            this.executor = ownedExecutor != null ? ownedExecutor : configuration.executor().orElseThrow();
        }

        /**
         * Creates the configured number of consumers, which hand the events they consume to the given
         * {@code subscriber}, and starts them once all are created. When this fails, the caller {@link #stop() stops}
         * these members, which stops the consumers started so far.
         */
        private void start(BiFunction<List<? extends EventMessage>, @Nullable ProcessingContext, CompletableFuture<?>>
                                   subscriber) {
            Map<String, Object> properties = ConsumerSettings.forGroup(configuration.consumerProperties(),
                                                                       configuration.groupId());
            for (int i = 0; i < configuration.consumerCount(); i++) {
                Map<String, Object> memberProperties = memberProperties(properties, i);
                // Every consumer the member creates, also after a failure, keeps the member's client identifier.
                KafkaClientFactory memberClientFactory = clientFactory.forClient(String.valueOf(i));
                GroupConsumer<V> member = new GroupConsumer<>(
                        () -> memberClientFactory.createConsumer(memberProperties,
                                                                 new StringDeserializer(),
                                                                 codec.valueSerde().deserializer()),
                        configuration,
                        codec,
                        subscriber,
                        unitOfWorkFactory,
                        executor,
                        deadLetterTopicWriter,
                        entitlementManager
                );
                member.failure().whenComplete((result, error) -> {
                    if (error != null) {
                        stopAfterFailure(this, error);
                    }
                });
                members.add(member);
            }
            for (int i = 0; i < members.size(); i++) {
                members.get(i).start("KafkaSubscribableEventSource-" + configuration.groupId() + "-" + i);
            }
        }

        /**
         * Returns the consumer settings of the member with the given {@code index}. Static membership requires a
         * group instance identifier of its own for every consumer, so a configured {@code group.instance.id} is
         * suffixed with the member's index when the source runs several consumers.
         */
        private Map<String, Object> memberProperties(Map<String, Object> properties, int index) {
            Object instanceId = properties.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
            if (instanceId == null || configuration.consumerCount() == 1) {
                return properties;
            }
            Map<String, Object> memberProperties = new HashMap<>(properties);
            memberProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId + "-" + index);
            return memberProperties;
        }

        private List<StoppedPartition> stoppedPartitions() {
            return members.stream()
                          .flatMap(member -> member.stoppedPartitions().stream())
                          .sorted(Comparator.comparing((StoppedPartition stopped) -> stopped.partition().topic())
                                            .thenComparingInt(stopped -> stopped.partition().partition()))
                          .toList();
        }

        private CompletableFuture<Void> stop() {
            members.forEach(GroupConsumer::requestStop);
            Duration timeout = ShutdownBudget.stopTimeout(configuration.revocationTimeout());
            long deadline = System.nanoTime() + timeout.toNanos();
            return CompletableFuture.runAsync(() -> {
                for (GroupConsumer<V> member : members) {
                    if (!member.awaitStop(remainingUntil(deadline, timeout))) {
                        logger.warn("A consumer of consumer group [{}] did not stop within {}.",
                                    configuration.groupId(), timeout);
                    }
                }
                if (ownedExecutor != null) {
                    ownedExecutor.shutdown();
                }
                // Within what is left of the members' stop timeout.
                deadLetterTopicWriter.close(remainingUntil(deadline, ShutdownBudget.DEAD_LETTER_CLOSE_TIMEOUT));
            }, Thread.ofVirtual().name("KafkaSubscribableEventSource-stop-" + configuration.groupId())::start);
        }

        /**
         * Returns the time left until the given {@code deadline}, a {@link System#nanoTime()} value, at most the given
         * {@code cap}.
         */
        private static Duration remainingUntil(long deadline, Duration cap) {
            return Duration.ofNanos(Math.clamp(deadline - System.nanoTime(), 0L, cap.toNanos()));
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("subscription", configuration.subscription());
        descriptor.describeProperty("groupId", configuration.groupId());
        descriptor.describeProperty("codec", configuration.codec());
        descriptor.describeProperty("consumerCount", configuration.consumerCount());
        descriptor.describeProperty("failedRecordPolicy", configuration.failedRecordPolicy());
        descriptor.describeProperty("retryInitialBackoff", configuration.retryInitialBackoff());
        descriptor.describeProperty("retryMaxBackoff", configuration.retryMaxBackoff());
        descriptor.describeProperty("undecodableRecordPolicy", configuration.undecodableRecordPolicy());
        descriptor.describeProperty("retentionGapPolicy", configuration.retentionGapPolicy());
        descriptor.describeProperty("initialPosition", configuration.initialPosition());
        descriptor.describeProperty("deadLetterTopicSuffix", configuration.deadLetterTopicSuffix());
        descriptor.describeProperty("pollTimeout", configuration.pollTimeout());
        descriptor.describeProperty("revocationTimeout", configuration.revocationTimeout());
        configuration.executor().ifPresent(executor -> descriptor.describeProperty("executor", executor));
    }
}
