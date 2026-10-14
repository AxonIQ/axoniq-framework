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
import io.axoniq.framework.springcloud.shared.InstanceAnswers.InstanceAnswer;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Discovers the members of the cluster through Spring Cloud Discovery, and resolves the member a command or query is
 * routed to from what it discovered.
 * <p>
 * This is what both connectors route with and publish their capabilities through. Commands and queries are routed
 * differently: a command is hashed onto a {@link ConsistentHash} ring by its routing key, so that commands for one
 * entity keep landing on one member, while a query carries no routing key and goes to whichever member advertises its
 * name, rotating over them to spread the load. The administration behind that, which members there are and what each
 * of them handles, is kept in a {@link MemberRegistry} this discovery owns.
 * <p>
 * Who is in the cluster and what each member handles are learned separately, because they change for different
 * reasons and are signalled differently:
 * <ul>
 *     <li><em>Membership</em> comes from the discovery implementation, which signals a change with a
 *     {@code HeartbeatEvent}. Eureka publishes one on every registry fetch, Spring Cloud Kubernetes whenever the set of
 *     endpoints changes. On each, {@link #updateMemberships()} reads the instances discovery reports, asks the ones it
 *     has not heard from for their capabilities, and drops the ones that are gone.</li>
 *     <li><em>Capabilities</em> change when a member subscribes or unsubscribes a handler, which no discovery
 *     implementation signals. Each instance's answer is therefore kept for a refresh interval, after which
 *     {@link #refreshCapabilities()} asks that instance again, without involving discovery.</li>
 * </ul>
 * After either, the ring is derived anew from the members as now known, so a member that has gone away simply does
 * not reappear. Both run one at a time, and {@link #startMembershipRound()} runs one on the {@link Executor} this
 * discovery is given, so a heartbeat never waits on the requests it causes.
 * <p>
 * Members are identified by the node id each one reports about itself, not by what discovery reports about them. An
 * application that discovery reports more than once, for example under two service ids, is one member, reached at the
 * first of those instances in discovery's own order. This application recognizes its own instance the same way: it is
 * the one answering with this application's node id. No Spring Cloud {@code Registration} is needed for that, so the
 * connector works the same with discovery implementations that register the application, such as Eureka, and with
 * those that leave registration to the platform, such as Spring Cloud Kubernetes.
 * <p>
 * Registered by the Spring Boot autoconfiguration, which starts a membership round on every {@code HeartbeatEvent}
 * and {@code InstanceRegisteredEvent}, and schedules the capability refresh. Both connectors share one, so that each
 * publishes what it handles without erasing what the other published.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudMemberDiscovery implements DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudMemberDiscovery.class);

    /**
     * The default for how long an instance's capabilities are taken as current before it is asked again: 30 seconds,
     * Eureka's default registry fetch interval, at which capabilities used to be refreshed along with membership.
     */
    public static final Duration DEFAULT_CAPABILITIES_REFRESH_INTERVAL = Duration.ofSeconds(30);

    /**
     * A backstop on how long one discovery round may take.
     * <p>
     * The real bound is the per-request timeout on the client asking for capabilities; this only stops a round from
     * outliving every heartbeat behind it should a client be configured without one.
     */
    private static final Duration DISCOVERY_ROUND_TIMEOUT = Duration.ofSeconds(30);

    private final MemberRegistry registry;
    private final DiscoveryClient discoveryClient;
    private final CapabilityDiscoveryMode discoveryMode;
    private final Predicate<ServiceInstance> serviceInstanceFilter;
    private final @Nullable String contextRootMetadataPropertyName;
    private final Duration capabilitiesRefreshInterval;
    private final Executor executor;

    // Replaced as a whole under this object's monitor, and recorded in the registry under it too, so that a round
    // merging its answers and a member being marked unreachable reach the registry in the order they happened.
    private volatile InstanceAnswers answers = InstanceAnswers.NONE;
    // Rounds take as long as their slowest request, so they are kept from overlapping by a lock of their own, rather
    // than by the monitor, which marking a member unreachable must never wait on for that long.
    private final ReentrantLock roundLock = new ReentrantLock();
    private final AtomicBoolean refreshPending = new AtomicBoolean();

    /**
     * Constructs a {@code SpringCloudMemberDiscovery} discovering members through the given {@code discoveryClient}.
     * <p>
     * Capabilities are refreshed after {@link #DEFAULT_CAPABILITIES_REFRESH_INTERVAL}, and rounds run on virtual
     * threads.
     *
     * @param discoveryClient the client reporting the service instances making up the cluster
     * @param discoveryMode   the mode used to learn which member each discovered instance is, and what it handles
     */
    public SpringCloudMemberDiscovery(DiscoveryClient discoveryClient, CapabilityDiscoveryMode discoveryMode) {
        this(discoveryClient,
             discoveryMode,
             instance -> true,
             null,
             DEFAULT_CAPABILITIES_REFRESH_INTERVAL,
             Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Constructs a {@code SpringCloudMemberDiscovery} discovering members through the given {@code discoveryClient}.
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
     *                                        round, and leaves
     *                                        {@link #scheduleCapabilityRefresh(ScheduledExecutorService)} nothing to
     *                                        schedule
     * @param executor                        runs the rounds {@link #startMembershipRound()} or the capability refresh
     *                                        starts, and the capability requests within them. A round waits for its
     *                                        requests, so the executor must run more than one task at a time
     */
    public SpringCloudMemberDiscovery(DiscoveryClient discoveryClient,
                                      CapabilityDiscoveryMode discoveryMode,
                                      Predicate<ServiceInstance> serviceInstanceFilter,
                                      @Nullable String contextRootMetadataPropertyName,
                                      Duration capabilitiesRefreshInterval,
                                      Executor executor) {
        this.discoveryClient = Objects.requireNonNull(discoveryClient, "The discoveryClient must not be null.");
        this.discoveryMode = Objects.requireNonNull(discoveryMode, "The discoveryMode must not be null.");
        this.serviceInstanceFilter = Objects.requireNonNull(serviceInstanceFilter,
                                                            "The serviceInstanceFilter must not be null.");
        this.contextRootMetadataPropertyName = contextRootMetadataPropertyName;
        this.capabilitiesRefreshInterval = Objects.requireNonNull(capabilitiesRefreshInterval,
                                                                  "The capabilitiesRefreshInterval must not be null.");
        this.executor = Objects.requireNonNull(executor, "The executor must not be null.");
        this.registry = new MemberRegistry(Member.localMember(discoveryMode.localNodeId()));
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
        return registry.findCommandDestination(routingKey, commandName);
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
        return registry.findQueryDestination(queryName);
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
        return registry.findAllQueryDestinations(queryName);
    }

    /**
     * Registers a {@code listener} to be notified after every change to the ring, with the ring that replaced the
     * previous one.
     * <p>
     * A subscription query relies on this: it has to know when a member that advertises its name appears, because it
     * cannot have received the updates that member emitted before it was subscribed to. Notified outside the lock the
     * ring is replaced under, so a listener may read this discovery freely. It runs on whichever thread made the
     * change, which for a discovery round is the thread running that round.
     *
     * @param listener notified with the new ring after every membership change
     * @return a registration cancelling the notification
     */
    // Fully qualified because Spring Cloud has a Registration of its own, which readers of this class may expect here.
    public org.axonframework.common.Registration onMembershipChanged(Consumer<ConsistentHash> listener) {
        return registry.onMembershipChanged(listener);
    }

    /**
     * Publishes the given {@code commands} as the commands this application handles, asking for the given
     * {@code loadFactor} worth of the command load.
     * <p>
     * Reflected in this application's own ring right away, so that a command dispatched right after its handler
     * subscribed can already be routed here, and visible to other members on their next round.
     *
     * @param loadFactor the share of the command load this application asks for
     * @param commands   the names of the commands this application handles
     */
    public void publishLocalCommands(int loadFactor, Set<QualifiedName> commands) {
        synchronized (this) {
            discoveryMode.updateLocalCapabilities(registry.publishLocalCommands(loadFactor, commands));
        }
    }

    /**
     * Publishes the given {@code queries} as the queries this application handles.
     * <p>
     * Reflected in this application's own ring right away, and visible to other members on their next round.
     *
     * @param queries the names of the queries this application handles
     */
    public void publishLocalQueries(Set<QualifiedName> queries) {
        synchronized (this) {
            discoveryMode.updateLocalCapabilities(registry.publishLocalQueries(queries));
        }
    }

    /**
     * Returns the {@link Member} representing this application, identified by this application's node id.
     *
     * @return the member representing this application
     */
    public Member localMember() {
        return registry.localMember();
    }

    /**
     * Returns every member currently known, this application included when it is part of the cluster, whatever they
     * handle.
     *
     * @return every member currently known, in no particular order
     */
    public Set<Member> members() {
        return registry.ring().members();
    }

    /**
     * Returns what the given {@code member} handles, as currently known.
     *
     * @param member the member to look up the capabilities of
     * @return the capabilities of the given {@code member}, or {@link Optional#empty()} when it is not a known member
     */
    public Optional<MemberCapabilities> capabilitiesOf(Member member) {
        return registry.ring().capabilitiesOf(member);
    }

    /**
     * Starts a membership round on this discovery's executor, as discovery signalled that the instances it reports may
     * have changed.
     * <p>
     * The thread signalling the change does not wait on the capability requests the round makes.
     */
    public void startMembershipRound() {
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
     * Exposed so that a deployment whose discovery implementation publishes no heartbeats, or a test, can drive
     * membership itself. Runs on the calling thread, after any round in progress has finished.
     */
    public void updateMemberships() {
        roundLock.lock();
        try {
            round(discoveredInstances());
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * Asks the instances whose capabilities are older than the capabilities refresh interval, or that have not
     * answered before, for their capabilities again.
     * <p>
     * Does not consult discovery: which instances there are is up to {@link #updateMemberships()}. This is what keeps
     * the ring current with discovery implementations that only signal membership changes, such as Spring Cloud
     * Kubernetes, while a member subscribes or unsubscribes handlers. Runs on the calling thread, after any round in
     * progress has finished.
     */
    public void refreshCapabilities() {
        roundLock.lock();
        try {
            round(answers.instances());
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * Asks those of the given {@code instances} that are due for their capabilities, and records the members that are
     * then known. Must be called while holding the {@link #roundLock}.
     *
     * @param instances the instances making up the cluster, in discovery's own order
     */
    private void round(List<ServiceInstance> instances) {
        List<ServiceInstance> due = answers.due(instances, System.nanoTime(), capabilitiesRefreshInterval);
        Map<ServiceInstanceKey, InstanceAnswer> asked = ask(due);
        synchronized (this) {
            answers = answers.merge(instances, asked);
            InstanceAnswers.Members members = answers.members(registry.localMember().name(), this::endpointOf);
            registry.replaceRemoteMembers(members.remote(), members.includesLocal());
        }
        discoveryMode.retainOnly(answers.keys());
        logger.debug("Updated the members from [{}] instances, of which [{}] were asked.",
                     instances.size(), due.size());
    }

    /**
     * Schedules {@link #refreshCapabilities()} on the given {@code scheduler}, twice per capabilities refresh interval,
     * so that no instance's capabilities are older than one and a half intervals.
     * <p>
     * The {@code scheduler} only decides when a refresh is due. The refresh itself runs on this discovery's executor,
     * and is skipped while the previous one has not finished, so the {@code scheduler} may be shared with other work
     * that must not be held up. Does nothing when the capabilities refresh interval is zero or negative.
     *
     * @param scheduler the scheduler deciding when a refresh is due
     * @return a registration cancelling the schedule
     */
    // Fully qualified because Spring Cloud has a Registration of its own, which readers of this class may expect here.
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

    /**
     * Takes the given {@code member} off the ring, as a message could not be delivered to it.
     * <p>
     * What the member answered is forgotten as well, so that the next round does not put it back before it has
     * answered again. It is asked again on that round, so this only keeps messages away from a member while it is
     * unreachable. It does not weaken any consistency guarantee: routing a command to one member is a matter of
     * locality, while consistency is enforced where the events are appended.
     *
     * @param member the member that could not be reached
     */
    public void markUnreachable(Member member) {
        Objects.requireNonNull(member, "The member must not be null.");
        synchronized (this) {
            answers = answers.forget(member);
            registry.remove(member);
        }
    }

    /**
     * Asks every given instance which member it is and what it handles.
     * <p>
     * The requests run concurrently on this discovery's executor. Asking each instance in turn would let one
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

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("capabilitiesRefreshInterval", capabilitiesRefreshInterval.toString());
        descriptor.describeProperty("registry", registry);
    }
}
