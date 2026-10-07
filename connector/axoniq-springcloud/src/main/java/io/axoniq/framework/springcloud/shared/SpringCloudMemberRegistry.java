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

package io.axoniq.framework.springcloud.shared;

import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.MemberAdvertisement;
import io.axoniq.framework.springcloud.discovery.ServiceInstanceKey;
import io.axoniq.framework.springcloud.routing.ConsistentHash;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Maintains the {@link ConsistentHash} ring this application routes commands with, and the members it routes queries
 * to, from the {@code ServiceInstance}s reported by Spring Cloud Discovery.
 * <p>
 * Commands and queries are routed differently, which is why both live here. A command is hashed onto the ring by its
 * routing key, so that commands for one entity keep landing on one member. A query carries no routing key, so it goes
 * to whichever member advertises its name, rotating over them to spread the load.
 * <p>
 * Who is in the cluster and what each member handles are learned separately, because they change for different
 * reasons and are signalled differently:
 * <ul>
 *     <li><em>Membership</em> comes from the discovery implementation, which signals a change with a
 *     {@link HeartbeatEvent}. Eureka publishes one on every registry fetch, Spring Cloud Kubernetes whenever the set of
 *     endpoints changes. On each, {@link #updateMemberships()} reads the instances discovery reports, asks the ones it
 *     has not heard from for their capabilities, and drops the ones that are gone.</li>
 *     <li><em>Capabilities</em> change when a member subscribes or unsubscribes a handler, which no discovery
 *     implementation signals. Each instance's answer is therefore kept for a refresh interval, after which
 *     {@link #refreshCapabilities()} asks that instance again, without involving discovery.</li>
 * </ul>
 * The ring is rebuilt from scratch from what is known after either, so a member that has gone away simply does not
 * reappear. Both run on the {@link Executor} this registry is given, one at a time, so a heartbeat never waits on the
 * requests it causes.
 * <p>
 * Members are identified by the node id each one reports about itself, not by what discovery reports about them. An
 * application that discovery reports more than once, for example under two service ids, is one member, reached at the
 * first of those instances in discovery's own order. This application recognizes its own instance the same way: it is
 * the one answering with this application's node id. No Spring Cloud {@code Registration} is needed for that, so the
 * connector works the same with discovery implementations that register the application, such as Eureka, and with
 * those that leave registration to the platform, such as Spring Cloud Kubernetes.
 * <p>
 * This registry must be a Spring bean: it learns about the cluster through {@link EventListener}-annotated methods,
 * and Spring only publishes events to beans it manages. It is registered by the Spring Boot autoconfiguration.
 * <p>
 * In Axon Framework 4 this work sat in a {@code CommandRouter} alongside the connector. That split is gone — a
 * connector now owns routing as well as transport — so this registry is the connectors' own collaborator rather than
 * a component of a bus. Both the command and the query connector share one, so that each publishes what it handles
 * without erasing what the other published.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudMemberRegistry implements DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudMemberRegistry.class);

    /**
     * A backstop on how long one discovery round may take.
     * <p>
     * The real bound is the per-request timeout on the client asking for capabilities; this only stops a round from
     * outliving every heartbeat behind it should a client be configured without one.
     */
    private static final Duration DISCOVERY_ROUND_TIMEOUT = Duration.ofSeconds(30);

    /**
     * The default for how long an instance's capabilities are taken as current before it is asked again: 30 seconds,
     * Eureka's default registry fetch interval, at which capabilities used to be refreshed along with membership.
     */
    public static final Duration DEFAULT_CAPABILITIES_REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final DiscoveryClient discoveryClient;
    private final CapabilityDiscoveryMode discoveryMode;
    private final Member localMember;
    private final Predicate<ServiceInstance> serviceInstanceFilter;
    private final @Nullable String contextRootMetadataPropertyName;
    private final Duration capabilitiesRefreshInterval;
    private final Executor executor;

    private volatile ConsistentHash ring = new ConsistentHash();
    private volatile MemberCapabilities localCapabilities = MemberCapabilities.INCAPABLE;
    // The last answer of every instance discovery reports, in discovery's own order. Replaced as a whole under this
    // object's monitor, so that a round merging its answers does not erase what markUnreachable did meanwhile.
    private volatile Map<ServiceInstanceKey, InstanceAnswer> answers = Map.of();
    // Rounds take as long as their slowest request, so they are kept from overlapping by a lock of their own, rather
    // than by the monitor the ring is replaced under, which dispatching threads must never wait on for that long.
    private final ReentrantLock roundLock = new ReentrantLock();
    private final AtomicBoolean refreshPending = new AtomicBoolean();

    // The parts of this member's capabilities are held separately because a connector only knows its own: publishing
    // the whole record from either would erase what the other had published.
    private volatile int localLoadFactor = 0;
    private volatile Set<QualifiedName> localCommands = Set.of();
    private volatile Set<QualifiedName> localQueries = Set.of();

    private final AtomicInteger queryRotation = new AtomicInteger();
    // Notified after every ring replacement. A subscription query has to know when a member that advertises its name
    // appears, because it cannot have received the updates that member emitted before it was subscribed to.
    private final List<Consumer<ConsistentHash>> membershipListeners = new CopyOnWriteArrayList<>();
    // Resolving a query's candidates means filtering and sorting every member, which is too much to repeat on every
    // dispatch. Replaced wholesale whenever the ring changes, rather than keyed on the ring: hashing a ring means
    // walking every member and every position it claims, which would cost more per dispatch than it saves.
    private volatile Map<QualifiedName, List<Member>> queryCandidates = new ConcurrentHashMap<>();

    /**
     * Constructs a {@code SpringCloudMemberRegistry} discovering members through the given {@code discoveryClient}.
     * <p>
     * Capabilities are refreshed after {@link #DEFAULT_CAPABILITIES_REFRESH_INTERVAL}, and rounds run on virtual
     * threads.
     *
     * @param discoveryClient the client reporting the service instances making up the cluster
     * @param discoveryMode   the mode used to learn which member each discovered instance is, and what it handles
     */
    public SpringCloudMemberRegistry(DiscoveryClient discoveryClient, CapabilityDiscoveryMode discoveryMode) {
        this(discoveryClient, discoveryMode, instance -> true, null);
    }

    /**
     * Constructs a {@code SpringCloudMemberRegistry} discovering members through the given {@code discoveryClient}.
     * <p>
     * Capabilities are refreshed after {@link #DEFAULT_CAPABILITIES_REFRESH_INTERVAL}, and rounds run on virtual
     * threads.
     *
     * @param discoveryClient                 the client reporting the service instances making up the cluster
     * @param discoveryMode                   the mode used to learn which member each discovered instance is, and
     *                                        what it handles
     * @param serviceInstanceFilter           decides which discovered instances are considered at all. Instances
     *                                        rejected here are never asked for their capabilities, which is cheaper
     *                                        than relying on the ignore list when whole services can be excluded up
     *                                        front.
     * @param contextRootMetadataPropertyName the {@link ServiceInstance#getMetadata() metadata} key holding an
     *                                        instance's context root, to be appended to its URI, or {@code null} when
     *                                        instances are served from the root
     */
    public SpringCloudMemberRegistry(DiscoveryClient discoveryClient,
                                     CapabilityDiscoveryMode discoveryMode,
                                     Predicate<ServiceInstance> serviceInstanceFilter,
                                     @Nullable String contextRootMetadataPropertyName) {
        this(discoveryClient,
             discoveryMode,
             serviceInstanceFilter,
             contextRootMetadataPropertyName,
             DEFAULT_CAPABILITIES_REFRESH_INTERVAL,
             Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Constructs a {@code SpringCloudMemberRegistry} discovering members through the given {@code discoveryClient}.
     *
     * @param discoveryClient                 the client reporting the service instances making up the cluster
     * @param discoveryMode                   the mode used to learn which member each discovered instance is, and
     *                                        what it handles
     * @param serviceInstanceFilter           decides which discovered instances are considered at all. Instances
     *                                        rejected here are never asked for their capabilities, which is cheaper
     *                                        than relying on the ignore list when whole services can be excluded up
     *                                        front.
     * @param contextRootMetadataPropertyName the {@link ServiceInstance#getMetadata() metadata} key holding an
     *                                        instance's context root, to be appended to its URI, or {@code null} when
     *                                        instances are served from the root
     * @param capabilitiesRefreshInterval     how long an instance's capabilities are taken as current before it is
     *                                        asked again. A zero or negative interval asks every instance on every
     *                                        membership round, and leaves {@link #refreshCapabilities()} nothing to do
     *                                        on a schedule
     * @param executor                        runs the rounds a {@link HeartbeatEvent} or the capability refresh
     *                                        starts, and the capability requests within them. A round waits for its
     *                                        requests, so the executor must run more than one task at a time
     */
    public SpringCloudMemberRegistry(DiscoveryClient discoveryClient,
                                     CapabilityDiscoveryMode discoveryMode,
                                     Predicate<ServiceInstance> serviceInstanceFilter,
                                     @Nullable String contextRootMetadataPropertyName,
                                     Duration capabilitiesRefreshInterval,
                                     Executor executor) {
        this.discoveryClient = Objects.requireNonNull(discoveryClient, "The discoveryClient must not be null.");
        this.discoveryMode = Objects.requireNonNull(discoveryMode, "The discoveryMode must not be null.");
        this.localMember = Member.localMember(discoveryMode.localNodeId());
        this.serviceInstanceFilter = Objects.requireNonNull(serviceInstanceFilter,
                                                            "The serviceInstanceFilter must not be null.");
        this.contextRootMetadataPropertyName = contextRootMetadataPropertyName;
        this.capabilitiesRefreshInterval = Objects.requireNonNull(capabilitiesRefreshInterval,
                                                                  "The capabilitiesRefreshInterval must not be null.");
        this.executor = Objects.requireNonNull(executor, "The executor must not be null.");
    }

    /**
     * Resolves the member a command of the given {@code commandName} carrying the given {@code routingKey} should be
     * handled by.
     *
     * @param routingKey  the routing key of the command to resolve a member for
     * @param commandName the {@link QualifiedName} of the command to resolve a member for
     * @return the member that should handle the command, or {@link Optional#empty()} when no known member handles
     * commands of the given {@code commandName}
     */
    public Optional<Member> findCommandDestination(String routingKey, QualifiedName commandName) {
        Objects.requireNonNull(routingKey, "The routingKey must not be null.");
        Objects.requireNonNull(commandName, "The commandName must not be null.");
        return ring.memberFor(routingKey, commandName);
    }

    /**
     * Resolves the member a query of the given {@code queryName} should be handled by.
     * <p>
     * Unlike a command, a query carries no routing key and asks nothing of where it is handled, so any member
     * advertising the name will do. Successive queries of the same name therefore rotate over the members
     * advertising it, spreading the load rather than sending every query of a name to the same member. Members are
     * rotated over in name order, so that the rotation is over a stable sequence even as the ring changes.
     * <p>
     * This member is one of the candidates like any other. Preferring the local handler is a policy of the
     * {@code DistributedQueryBus}, which applies it before the query reaches a connector at all.
     *
     * @param queryName the {@link QualifiedName} of the query to resolve a member for
     * @return the member that should handle the query, or {@link Optional#empty()} when no known member handles
     * queries of the given {@code queryName}
     */
    public Optional<Member> findQueryDestination(QualifiedName queryName) {
        Objects.requireNonNull(queryName, "The queryName must not be null.");
        // Read once: a ring change replaces the whole map, and resolving against the map that was current when this
        // query started is correct, because that is the ring that was current when it started.
        Map<QualifiedName, List<Member>> candidatesByName = queryCandidates;
        ConsistentHash currentRing = ring;
        List<Member> candidates =
                candidatesByName.computeIfAbsent(queryName, name -> candidatesFor(currentRing, name));
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        int index = Math.floorMod(queryRotation.getAndIncrement(), candidates.size());
        return Optional.of(candidates.get(index));
    }

    /**
     * Returns every member advertising the given {@code queryName}, in a stable order.
     * <p>
     * Where {@link #findQueryDestination(QualifiedName)} picks one member to answer a query, a subscription query
     * needs them all: an update is emitted on whichever member's state changed, and a member only matches a
     * subscription it holds a registration for. A subscriber that reached only one member would miss every update
     * emitted on the others.
     *
     * @param queryName the {@link QualifiedName} of the query to resolve members for
     * @return every member advertising the given {@code queryName}, empty when none does
     */
    public List<Member> findAllQueryDestinations(QualifiedName queryName) {
        Objects.requireNonNull(queryName, "The queryName must not be null.");
        Map<QualifiedName, List<Member>> candidatesByName = queryCandidates;
        ConsistentHash currentRing = ring;
        return candidatesByName.computeIfAbsent(queryName, name -> candidatesFor(currentRing, name));
    }

    /**
     * Registers a {@code listener} to be notified after every change to the ring, with the ring that replaced the
     * previous one.
     * <p>
     * Notified outside the lock the ring is replaced under, so a listener may read the registry freely. It runs on
     * whichever thread made the change, which for a discovery round is the thread delivering the heartbeat.
     *
     * @param listener notified with the new ring after every membership change
     * @return a registration cancelling the notification
     */
    // Fully qualified because Spring Cloud has a Registration of its own, which readers of this class may expect here.
    public org.axonframework.common.Registration onMembershipChanged(Consumer<ConsistentHash> listener) {
        Objects.requireNonNull(listener, "The listener must not be null.");
        membershipListeners.add(listener);
        return () -> membershipListeners.remove(listener);
    }

    /**
     * Notifies the membership listeners of the given {@code current} ring.
     * <p>
     * A listener that throws must not stop the others from being told, nor break the discovery round that led here.
     *
     * @param current the ring that replaced the previous one
     */
    private void notifyMembershipChanged(ConsistentHash current) {
        for (Consumer<ConsistentHash> listener : membershipListeners) {
            try {
                listener.accept(current);
            } catch (Exception e) {
                logger.warn("A membership listener failed. The ring is unaffected.", e);
            }
        }
    }

    private static List<Member> candidatesFor(ConsistentHash ring, QualifiedName queryName) {
        return ring.members()
                   .stream()
                   .filter(member -> ring.capabilitiesOf(member)
                                         .filter(c -> c.handlesQuery(queryName))
                                         .isPresent())
                   .sorted(Comparator.comparing(Member::name))
                   .toList();
    }

    /**
     * Publishes the given {@code commands} as the commands this application handles, asking for the given
     * {@code loadFactor} worth of the command load.
     *
     * @param loadFactor the share of the command load this application asks for
     * @param commands   the names of the commands this application handles
     */
    public void publishLocalCommands(int loadFactor, Set<QualifiedName> commands) {
        if (loadFactor < 0) {
            throw new IllegalArgumentException("The load factor cannot be negative, but was [" + loadFactor + "].");
        }
        Objects.requireNonNull(commands, "The commands must not be null.");
        this.localLoadFactor = loadFactor;
        this.localCommands = Set.copyOf(commands);
        republishLocalCapabilities();
    }

    /**
     * Publishes the given {@code queries} as the queries this application handles.
     *
     * @param queries the names of the queries this application handles
     */
    public void publishLocalQueries(Set<QualifiedName> queries) {
        Objects.requireNonNull(queries, "The queries must not be null.");
        this.localQueries = Set.copyOf(queries);
        republishLocalCapabilities();
    }

    /**
     * Makes this application's capabilities visible to other members on their next discovery round, and reflects them
     * in this application's own ring right away.
     * <p>
     * Composed from the parts each connector published, so that a connector publishing what it handles leaves what the
     * other publishes untouched.
     */
    private void republishLocalCapabilities() {
        MemberCapabilities capabilities = currentLocalCapabilities();
        this.localCapabilities = capabilities;
        discoveryMode.updateLocalCapabilities(capabilities);
        // Updating the local member immediately, rather than waiting for the next heartbeat, means a message
        // dispatched right after its handler subscribed can already be routed to this member.
        ConsistentHash updated;
        synchronized (this) {
            updated = ring = ring.with(localMember, currentLocalCapabilities());
            queryCandidates = new ConcurrentHashMap<>();
        }
        logger.debug("Published local capabilities [{}]; ring is now [{}]", capabilities, ring);
        notifyMembershipChanged(updated);
    }

    /**
     * Composes this member's capabilities from the parts each connector published.
     * <p>
     * Read again inside the lock wherever the ring is updated, rather than passed in: a capability published while a
     * ring was being rebuilt would otherwise be overwritten by the rebuild, and stay invisible until the next
     * heartbeat.
     *
     * @return the capabilities this member currently has
     */
    private MemberCapabilities currentLocalCapabilities() {
        return new MemberCapabilities(localLoadFactor, localCommands, localQueries);
    }

    /**
     * Removes the given {@code member} from the ring, on the grounds that it could not be reached.
     * <p>
     * The member returns once it answers a capabilities request again, which it is asked on the next round, so this
     * only keeps commands away from a member that is currently unreachable. It does not weaken any consistency
     * guarantee: routing a command to one member is a matter of locality, while consistency is enforced where the
     * events are appended.
     *
     * @param member the member that could not be reached
     */
    public void markUnreachable(Member member) {
        Objects.requireNonNull(member, "The member must not be null.");
        synchronized (this) {
            // Forgetting what the member answered, rather than only taking it off the ring, so that the next rebuild
            // does not put it back before it has answered again.
            Map<ServiceInstanceKey, InstanceAnswer> updatedAnswers = new LinkedHashMap<>(answers);
            updatedAnswers.replaceAll((key, answer) -> answer.isFrom(member) ? answer.forgotten() : answer);
            answers = updatedAnswers;
            ConsistentHash updated = ring.without(member);
            if (updated != ring) {
                logger.info("Removing member [{}] from the ring, as it could not be reached. It returns once it "
                                    + "answers again.", member);
                ring = updated;
                queryCandidates = new ConcurrentHashMap<>();
            }
        }
    }

    /**
     * Starts a membership round as soon as this application has registered with discovery, rather than waiting for the
     * next heartbeat.
     * <p>
     * Only published by discovery implementations that register the application themselves. Others leave this
     * application to be found on the next membership round.
     *
     * @param event the event signalling that registration completed. Serves only as a trigger
     */
    @EventListener
    public void onInstanceRegistered(InstanceRegisteredEvent<?> event) {
        logger.debug("This instance completed its discovery registration; starting a membership round.");
        runAsync(this::updateMemberships);
    }

    /**
     * Starts a membership round, as discovery signalled that the instances it reports may have changed.
     * <p>
     * The round runs on this registry's executor, so the thread delivering the heartbeat does not wait on the
     * capability requests it causes.
     *
     * @param event the heartbeat signalling that discovery may have new information. Serves only as a trigger
     */
    @EventListener
    public void onHeartbeat(HeartbeatEvent event) {
        runAsync(this::updateMemberships);
    }

    private void runAsync(Runnable round) {
        try {
            executor.execute(() -> {
                try {
                    round.run();
                } catch (RuntimeException e) {
                    logger.warn("A discovery round failed; the ring is left as it is until the next one.", e);
                }
            });
        } catch (RejectedExecutionException e) {
            logger.debug("A discovery round was not started, as the executor no longer accepts work.", e);
        }
    }

    /**
     * Brings the ring in line with the instances discovery currently reports.
     * <p>
     * Instances discovery no longer reports are dropped. Instances that are new, that have not answered before, or
     * whose answer is older than the capabilities refresh interval are asked for their capabilities; the others keep
     * what they answered before.
     * <p>
     * Called on every {@link HeartbeatEvent}, and exposed so that a deployment whose discovery implementation publishes
     * no heartbeats, or a test, can drive membership itself. Runs on the calling thread, after any round in progress
     * has finished.
     */
    public void updateMemberships() {
        roundLock.lock();
        try {
            List<ServiceInstance> instances = discoveredInstances();
            Map<ServiceInstanceKey, InstanceAnswer> known = answers;
            long now = System.nanoTime();
            List<ServiceInstance> due = instances.stream()
                                                 .filter(instance -> isDue(known.get(ServiceInstanceKey.of(instance)),
                                                                           now))
                                                 .toList();
            Map<ServiceInstanceKey, InstanceAnswer> asked = ask(due);
            ConsistentHash updated;
            synchronized (this) {
                Map<ServiceInstanceKey, InstanceAnswer> current = answers;
                Map<ServiceInstanceKey, InstanceAnswer> updatedAnswers = new LinkedHashMap<>();
                for (ServiceInstance instance : instances) {
                    ServiceInstanceKey key = ServiceInstanceKey.of(instance);
                    InstanceAnswer answer = Objects.requireNonNullElseGet(
                            asked.get(key),
                            () -> current.getOrDefault(key, InstanceAnswer.unanswered(instance))
                    );
                    // Taking the instance as discovery reports it now, which may carry newer metadata.
                    updatedAnswers.putIfAbsent(key, answer.reportedAs(instance));
                }
                answers = updatedAnswers;
                updated = rebuildRing();
            }
            discoveryMode.retainOnly(Set.copyOf(answers.keySet()));
            notifyMembershipChanged(updated);
            logger.debug("Rebuilt the ring from [{}] discovered instances, of which [{}] were asked: [{}]",
                         instances.size(), due.size(), updated);
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * Asks the instances whose capabilities are older than the capabilities refresh interval, or that have not
     * answered before, for their capabilities again, and rebuilds the ring when any were asked.
     * <p>
     * Does not consult discovery: which instances there are is up to {@link #updateMemberships()}. This is what keeps
     * the ring current with discovery implementations that only signal membership changes, such as Spring Cloud
     * Kubernetes, while a member subscribes or unsubscribes handlers. Runs on the calling thread, after any round in
     * progress has finished.
     */
    public void refreshCapabilities() {
        roundLock.lock();
        try {
            long now = System.nanoTime();
            List<ServiceInstance> due = answers.values().stream()
                                               .filter(answer -> isDue(answer, now))
                                               .map(InstanceAnswer::instance)
                                               .toList();
            if (due.isEmpty()) {
                return;
            }
            Map<ServiceInstanceKey, InstanceAnswer> asked = ask(due);
            ConsistentHash updated;
            synchronized (this) {
                Map<ServiceInstanceKey, InstanceAnswer> updatedAnswers = new LinkedHashMap<>(answers);
                asked.forEach((key, answer) -> updatedAnswers.computeIfPresent(key, (k, previous) -> answer));
                answers = updatedAnswers;
                updated = rebuildRing();
            }
            notifyMembershipChanged(updated);
            logger.debug("Refreshed the capabilities of [{}] instances: [{}]", due.size(), updated);
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * Schedules {@link #refreshCapabilities()} on the given {@code scheduler}, twice per capabilities refresh interval,
     * so that no instance's capabilities are older than one and a half intervals.
     * <p>
     * The {@code scheduler} only decides when a refresh is due. The refresh itself runs on this registry's executor,
     * and is skipped while the previous one has not finished, so the {@code scheduler} may be shared with other work
     * that must not be held up. Does nothing when the capabilities refresh interval is zero or negative.
     *
     * @param scheduler the scheduler deciding when a refresh is due
     * @return a registration cancelling the schedule
     */
    public org.axonframework.common.Registration scheduleCapabilityRefresh(ScheduledExecutorService scheduler) {
        Objects.requireNonNull(scheduler, "The scheduler must not be null.");
        if (!capabilitiesRefreshInterval.isPositive()) {
            return () -> true;
        }
        long period = Math.max(1, capabilitiesRefreshInterval.toMillis() / 2);
        ScheduledFuture<?> schedule = scheduler.scheduleWithFixedDelay(() -> {
            if (refreshPending.compareAndSet(false, true)) {
                runAsync(() -> {
                    try {
                        refreshCapabilities();
                    } finally {
                        refreshPending.set(false);
                    }
                });
            }
        }, period, period, TimeUnit.MILLISECONDS);
        return () -> schedule.cancel(false);
    }

    private boolean isDue(@Nullable InstanceAnswer answer, long now) {
        return answer == null
                || answer.advertisement() == null
                || now - answer.answeredAt() >= capabilitiesRefreshInterval.toNanos();
    }

    /**
     * Asks every given instance which member it is and what it handles.
     * <p>
     * The requests run concurrently on this registry's executor. Asking each instance in turn would let one
     * unresponsive instance delay every instance behind it; the requests are independent, so the round costs the
     * slowest single response rather than the sum of all of them. An instance that has not answered within
     * {@link #DISCOVERY_ROUND_TIMEOUT} is taken as not having answered.
     *
     * @param instances the instances to ask
     * @return the answer of each instance asked, by its key
     */
    private Map<ServiceInstanceKey, InstanceAnswer> ask(List<ServiceInstance> instances) {
        if (instances.isEmpty()) {
            return Map.of();
        }
        long askedAt = System.nanoTime();
        List<CompletableFuture<Optional<MemberAdvertisement>>> requests = new ArrayList<>();
        for (ServiceInstance instance : instances) {
            try {
                requests.add(CompletableFuture.supplyAsync(() -> advertisementOf(instance), executor));
            } catch (RejectedExecutionException e) {
                requests.add(CompletableFuture.completedFuture(Optional.empty()));
            }
        }
        CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new))
                         .completeOnTimeout(null, DISCOVERY_ROUND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                         .join();
        Map<ServiceInstanceKey, InstanceAnswer> asked = new LinkedHashMap<>();
        for (int i = 0; i < instances.size(); i++) {
            ServiceInstance instance = instances.get(i);
            CompletableFuture<Optional<MemberAdvertisement>> request = requests.get(i);
            Optional<MemberAdvertisement> advertisement;
            if (request.isDone()) {
                advertisement = request.join();
            } else {
                request.cancel(true);
                logger.info("Leaving ServiceInstance [{}] out of the ring, as it did not answer within {}.",
                            ServiceInstanceKey.of(instance), DISCOVERY_ROUND_TIMEOUT);
                advertisement = Optional.empty();
            }
            asked.put(ServiceInstanceKey.of(instance),
                      new InstanceAnswer(instance, advertisement.orElse(null), askedAt));
        }
        return asked;
    }

    /**
     * Rebuilds the ring from the answers known for every instance discovery reports. Must be called while holding this
     * object's monitor.
     * <p>
     * Answers are taken in discovery's own order. Instances answering with the same node id are one member, reached at
     * the first of them, so every member of the cluster picks the same one. The instance answering with this
     * application's own node id is the {@link #localMember() local member}.
     *
     * @return the rebuilt ring
     */
    private ConsistentHash rebuildRing() {
        Map<ServiceInstanceKey, InstanceAnswer> current = answers;
        Map<String, Member> membersByNodeId = new LinkedHashMap<>();
        Map<Member, MemberCapabilities> memberships = new LinkedHashMap<>();
        boolean reportsLocal = false;
        for (InstanceAnswer answer : current.values()) {
            MemberAdvertisement advertisement = answer.advertisement();
            if (advertisement == null) {
                continue;
            }
            if (advertisement.nodeId().equals(localMember.name())) {
                reportsLocal = true;
                continue;
            }
            Member known = membersByNodeId.get(advertisement.nodeId());
            if (known != null) {
                logger.debug("ServiceInstance [{}] is member [{}], which is already reached at [{}].",
                             ServiceInstanceKey.of(answer.instance()), advertisement.nodeId(), known.endpoint());
                continue;
            }
            URI endpoint = endpointOf(answer.instance());
            if (endpoint == null) {
                // Only possible for an instance that answered without reporting where it is, such as one a discovery
                // implementation has not finished registering.
                continue;
            }
            Member member = new Member(advertisement.nodeId(), endpoint, false);
            membersByNodeId.put(advertisement.nodeId(), member);
            memberships.put(member, advertisement.capabilities());
        }
        ConsistentHash rebuilt = ring.withOnly(memberships);
        // This application is part of its own ring when discovery reports it, which is what every other member goes by
        // as well. A member discovery left out stays out, so that this application does not route commands to itself
        // that the rest of the cluster routes elsewhere. Only when discovery reports nothing at all does this
        // application keep itself, since it can handle its own commands while it waits for discovery to catch up. Its
        // capabilities are its current ones rather than what its instance answered, which may predate a handler that
        // subscribed since.
        ring = reportsLocal || current.isEmpty() ? rebuilt.with(localMember, currentLocalCapabilities()) : rebuilt;
        queryCandidates = new ConcurrentHashMap<>();
        return ring;
    }

    private Optional<MemberAdvertisement> advertisementOf(ServiceInstance instance) {
        try {
            return discoveryMode.discover(instance);
        } catch (Exception e) {
            logger.info("Leaving ServiceInstance [{}] out of the ring, as discovering its capabilities failed.",
                        ServiceInstanceKey.of(instance), e);
            return Optional.empty();
        }
    }

    private List<ServiceInstance> discoveredInstances() {
        return discoveryClient.getServices()
                              .stream()
                              .map(discoveryClient::getInstances)
                              .flatMap(Collection::stream)
                              .filter(serviceInstanceFilter)
                              .toList();
    }

    /**
     * Returns the {@link Member} representing this application, identified by this application's node id.
     *
     * @return the member representing this application
     */
    public Member localMember() {
        return localMember;
    }

    /**
     * Returns the ring this registry currently routes with.
     *
     * @return the current routing ring
     */
    public ConsistentHash ring() {
        return ring;
    }

    private @Nullable URI endpointOf(ServiceInstance instance) {
        URI uri = uriOf(instance);
        if (uri == null || contextRootMetadataPropertyName == null) {
            return uri;
        }
        String contextRoot = Optional.ofNullable(instance.getMetadata())
                                     .map(metadata -> metadata.get(contextRootMetadataPropertyName))
                                     .orElse(null);
        if (contextRoot == null) {
            logger.debug("ServiceInstance [{}] has no [{}] metadata property; serving it from the root.",
                         ServiceInstanceKey.of(instance), contextRootMetadataPropertyName);
            return uri;
        }
        return UriComponentsBuilder.fromUri(uri).path(contextRoot).build().toUri();
    }

    /**
     * Returns the URI of the given {@code instance}, or {@code null} when it does not have one yet.
     * <p>
     * Several Spring Cloud Discovery implementations throw rather than return {@code null} when an instance's URI is
     * requested before it has registered, so the exception is what "no URI yet" looks like in practice.
     *
     * @param instance the instance to read the URI of
     * @return the URI of the given {@code instance}, or {@code null} when it does not have one
     */
    private static @Nullable URI uriOf(ServiceInstance instance) {
        try {
            return instance.getUri();
        } catch (Exception e) {
            logger.debug("ServiceInstance [{}] does not report a URI yet.", instance.getServiceId(), e);
            return null;
        }
    }

    /**
     * What one instance discovery reports last answered, and when.
     *
     * @param instance      the instance as discovery last reported it
     * @param advertisement what the instance answered, or {@code null} when it has not answered, or its answer was
     *                      forgotten because the member could not be reached
     * @param answeredAt    the {@link System#nanoTime()} at which the instance was asked
     */
    private record InstanceAnswer(ServiceInstance instance,
                                  @Nullable MemberAdvertisement advertisement,
                                  long answeredAt) {

        private static InstanceAnswer unanswered(ServiceInstance instance) {
            return new InstanceAnswer(instance, null, 0);
        }

        private InstanceAnswer reportedAs(ServiceInstance reported) {
            return new InstanceAnswer(reported, advertisement, answeredAt);
        }

        private InstanceAnswer forgotten() {
            return new InstanceAnswer(instance, null, answeredAt);
        }

        private boolean isFrom(Member member) {
            return advertisement != null && advertisement.nodeId().equals(member.name());
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        ConsistentHash current = ring;
        descriptor.describeProperty("localMember", localMember.name());
        descriptor.describeProperty("localCapabilities", localCapabilities.toString());
        descriptor.describeProperty("ringVersion", current.version());
        descriptor.describeProperty("members", memberNames(current.members()));
    }

    private static List<String> memberNames(Set<Member> members) {
        return members.stream().map(Member::name).sorted().toList();
    }
}
