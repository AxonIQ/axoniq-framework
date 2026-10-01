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

package io.axoniq.framework.springcloud;

import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
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
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.event.EventListener;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * The ring is rebuilt from scratch on every {@link HeartbeatEvent}, which is what makes the discovery implementation's
 * heartbeat interval the speed at which cluster topology changes propagate. Rebuilding rather than patching means a
 * member that has gone away simply does not reappear, and no bookkeeping is needed to notice it left.
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

    private final DiscoveryClient discoveryClient;
    private final Registration localRegistration;
    private final CapabilityDiscoveryMode discoveryMode;
    private final Predicate<ServiceInstance> serviceInstanceFilter;
    private final @Nullable String contextRootMetadataPropertyName;

    private volatile ConsistentHash ring = new ConsistentHash();
    private volatile MemberCapabilities localCapabilities = MemberCapabilities.INCAPABLE;
    private volatile boolean registered = false;

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
     *
     * @param discoveryClient   the client reporting the service instances making up the cluster
     * @param localRegistration the registration representing this application, used to tell this application's own
     *                          instance apart from the others
     * @param discoveryMode     the mode used to learn what each discovered instance handles
     */
    public SpringCloudMemberRegistry(DiscoveryClient discoveryClient,
                                     Registration localRegistration,
                                     CapabilityDiscoveryMode discoveryMode) {
        this(discoveryClient, localRegistration, discoveryMode, instance -> true, null);
    }

    /**
     * Constructs a {@code SpringCloudMemberRegistry} discovering members through the given {@code discoveryClient}.
     *
     * @param discoveryClient                 the client reporting the service instances making up the cluster
     * @param localRegistration               the registration representing this application, used to tell this
     *                                        application's own instance apart from the others
     * @param discoveryMode                   the mode used to learn what each discovered instance handles
     * @param serviceInstanceFilter           decides which discovered instances are considered at all. Instances
     *                                        rejected here are never asked for their capabilities, which is cheaper
     *                                        than relying on the ignore list when whole services can be excluded up
     *                                        front.
     * @param contextRootMetadataPropertyName the {@link ServiceInstance#getMetadata() metadata} key holding an
     *                                        instance's context root, to be appended to its URI, or {@code null} when
     *                                        instances are served from the root
     */
    public SpringCloudMemberRegistry(DiscoveryClient discoveryClient,
                                     Registration localRegistration,
                                     CapabilityDiscoveryMode discoveryMode,
                                     Predicate<ServiceInstance> serviceInstanceFilter,
                                     @Nullable String contextRootMetadataPropertyName) {
        this.discoveryClient = Objects.requireNonNull(discoveryClient, "The discoveryClient must not be null.");
        this.localRegistration = Objects.requireNonNull(localRegistration, "The localRegistration must not be null.");
        this.discoveryMode = Objects.requireNonNull(discoveryMode, "The discoveryMode must not be null.");
        this.serviceInstanceFilter = Objects.requireNonNull(serviceInstanceFilter,
                                                            "The serviceInstanceFilter must not be null.");
        this.contextRootMetadataPropertyName = contextRootMetadataPropertyName;
        // Publishing the (empty) local capabilities up front means the discovery mode recognises this application's
        // own instance from the first discovery round, rather than asking this application for its capabilities over
        // HTTP until the first handler subscribes.
        this.discoveryMode.updateLocalCapabilities(localRegistration, MemberCapabilities.INCAPABLE);
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
    // Fully qualified because Spring Cloud's own Registration, which this class takes as its local registration, has
    // the same simple name.
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
        discoveryMode.updateLocalCapabilities(localRegistration, capabilities);
        // Updating the local member immediately, rather than waiting for the next heartbeat, means a message
        // dispatched right after its handler subscribed can already be routed to this member.
        ConsistentHash updated;
        synchronized (this) {
            updated = ring = ring.with(localMember(), currentLocalCapabilities());
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
     * The member returns on the next discovery round if it answers again, so this only keeps commands away from a
     * member that is currently unreachable. It does not weaken any consistency guarantee: routing a command to one
     * member is a matter of locality, while consistency is enforced where the events are appended.
     *
     * @param member the member that could not be reached
     */
    public void markUnreachable(Member member) {
        Objects.requireNonNull(member, "The member must not be null.");
        synchronized (this) {
            ConsistentHash updated = ring.without(member);
            if (updated != ring) {
                logger.info("Removing member [{}] from the ring, as it could not be reached. It returns on the next "
                                    + "discovery round if it answers again.", member);
                ring = updated;
                queryCandidates = new ConcurrentHashMap<>();
            }
        }
    }

    /**
     * Rebuilds the ring now that this application has completed its discovery registration.
     * <p>
     * Until registration completes, this application's own URI is not available, so it is a member of its own ring
     * under a provisional name and without an endpoint. This event is the point at which its real name and URI become
     * known.
     *
     * @param event the event signalling that registration completed. Serves only as a trigger
     */
    @EventListener
    public void onInstanceRegistered(InstanceRegisteredEvent<?> event) {
        logger.debug("This instance completed its discovery registration; rebuilding the ring.");
        registered = true;
        updateMemberships();
    }

    /**
     * Rebuilds the ring from the instances discovery currently reports.
     *
     * @param event the heartbeat signalling that discovery may have new information. Serves only as a trigger
     */
    @EventListener
    public void onHeartbeat(HeartbeatEvent event) {
        updateMemberships();
    }

    /**
     * Rebuilds the ring from the instances discovery currently reports, asking each for its capabilities.
     * <p>
     * Exposed beyond the event listeners so that a deployment without discovery heartbeats, or a test, can drive the
     * discovery round itself.
     */
    public void updateMemberships() {
        List<ServiceInstance> instances = discoveredInstances();
        Optional<Map<Member, MemberCapabilities>> discovered = discoverCapabilities(instances);
        if (discovered.isEmpty()) {
            // The round did not finish. Leaving the ring as it is beats rebuilding it from a partial answer, which
            // would drop every member this round had not reached yet.
            return;
        }
        ConsistentHash rebuilt = ring.withOnly(discovered.get());
        discoveryMode.retainOnly(instances.stream()
                                          .map(ServiceInstanceKey::of)
                                          .collect(Collectors.toUnmodifiableSet()));
        ConsistentHash updated;
        synchronized (this) {
            // Re-applying this member's own capabilities, rather than trusting the rebuild to carry them: the rebuild
            // read them before taking the lock, so a handler that subscribed in between would be lost until the next
            // heartbeat. Only refreshed where the rebuild already placed this member, so that a member discovery left
            // out stays out.
            Member local = localMember();
            updated = ring = rebuilt.capabilitiesOf(local).isPresent()
                    ? rebuilt.with(local, currentLocalCapabilities())
                    : rebuilt;
            queryCandidates = new ConcurrentHashMap<>();
        }
        notifyMembershipChanged(updated);
        logger.debug("Rebuilt the ring from [{}] discovered instances: [{}]", instances.size(), rebuilt);
    }

    /**
     * Asks every given instance what it handles, and returns the memberships to rebuild the ring from.
     * <p>
     * The requests run concurrently. Asking each instance in turn would let one unresponsive instance delay every
     * instance behind it, so that a round over a registry holding many instances outlasts the heartbeat interval and
     * the ring stops converging; the requests are independent, so the round costs the slowest single response rather
     * than the sum of all of them.
     * <p>
     * Results are collected in discovery's own order, not in completion order, so that two instances resolving to the
     * same member name resolve the same way on every member of the cluster.
     *
     * @param instances the instances discovery currently reports
     * @return the memberships to rebuild the ring from, or an empty {@code Optional} when the round did not complete
     */
    private Optional<Map<Member, MemberCapabilities>> discoverCapabilities(List<ServiceInstance> instances) {
        if (instances.isEmpty()) {
            return Optional.of(Map.of());
        }
        List<Callable<Optional<MemberCapabilities>>> requests =
                instances.stream()
                         .<Callable<Optional<MemberCapabilities>>>map(
                                 instance -> () -> capabilitiesOf(instance)
                         )
                         .toList();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Optional<MemberCapabilities>>> answers =
                    executor.invokeAll(requests, DISCOVERY_ROUND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<Member, MemberCapabilities> memberships = new LinkedHashMap<>();
            for (int i = 0; i < answers.size(); i++) {
                ServiceInstance instance = instances.get(i);
                capabilitiesFrom(answers.get(i), instance)
                        .ifPresent(capabilities -> memberships.put(buildMember(instance), capabilities));
            }
            return Optional.of(memberships);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("The discovery round was interrupted, so the ring is left as it is.");
            return Optional.empty();
        }
    }

    /**
     * Reads one instance's answer, treating a request that failed or never finished as an instance to leave out.
     *
     * @param answer   the pending answer to the capabilities request
     * @param instance the instance that was asked
     * @return what the instance handles, or an empty {@code Optional} when it could not be established
     */
    private Optional<MemberCapabilities> capabilitiesFrom(Future<Optional<MemberCapabilities>> answer,
                                                          ServiceInstance instance) {
        if (answer.isCancelled()) {
            logger.info("Leaving ServiceInstance [{}] out of the ring, as it did not answer within {}.",
                        ServiceInstanceKey.of(instance), DISCOVERY_ROUND_TIMEOUT);
            return Optional.empty();
        }
        try {
            return answer.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            logger.info("Leaving ServiceInstance [{}] out of the ring, as discovering its capabilities failed.",
                        ServiceInstanceKey.of(instance), e.getCause());
            return Optional.empty();
        }
    }

    private Optional<MemberCapabilities> capabilitiesOf(ServiceInstance instance) {
        try {
            return discoveryMode.capabilities(instance);
        } catch (Exception e) {
            logger.info("Leaving ServiceInstance [{}] out of the ring, as discovering its capabilities failed.",
                        ServiceInstanceKey.of(instance), e);
            return Optional.empty();
        }
    }

    private List<ServiceInstance> discoveredInstances() {
        List<ServiceInstance> instances = discoveryClient.getServices()
                                                        .stream()
                                                        .map(discoveryClient::getInstances)
                                                        .flatMap(Collection::stream)
                                                        .filter(serviceInstanceFilter)
                                                        .collect(Collectors.toCollection(ArrayList::new));
        if (instances.isEmpty()) {
            // Discovery may not have anything to report yet, but this application can already handle its own
            // commands, so it must not fall out of its own ring while it waits.
            instances.add(localRegistration);
        }
        return instances;
    }

    /**
     * Returns the {@link Member} representing this application.
     *
     * @return the member representing this application
     */
    public Member localMember() {
        if (!registered) {
            return Member.unregisteredLocalMember(provisionalName(localRegistration));
        }
        return buildMember(localRegistration);
    }

    /**
     * Returns the ring this registry currently routes with.
     *
     * @return the current routing ring
     */
    public ConsistentHash ring() {
        return ring;
    }

    private Member buildMember(ServiceInstance instance) {
        URI endpoint = endpointOf(instance);
        boolean local = isLocal(instance);
        if (endpoint == null) {
            // Only reachable for this application's own instance before registration completed; a remote instance
            // without a URI was already filtered out by its capabilities request failing.
            return Member.unregisteredLocalMember(provisionalName(instance));
        }
        return new Member(serviceName(instance) + "[" + endpoint + "]", endpoint, local);
    }

    /**
     * Returns the name a member goes by before its endpoint is known.
     *
     * @param instance the instance to name
     * @return the provisional name of the given {@code instance}
     */
    private static String provisionalName(ServiceInstance instance) {
        return serviceName(instance) + "[LOCAL]";
    }

    /**
     * Returns the service id of the given {@code instance}, upper-cased in a locale-independent way.
     * <p>
     * Member names are the ring's keys and travel between members, so they must not depend on the default locale of
     * the JVM that produced them: under a Turkish locale {@code toUpperCase()} maps {@code i} to a dotted capital,
     * which would give one member a different name on every other member of the cluster.
     *
     * @param instance the instance to name
     * @return the locale-independent upper-cased service id of the given {@code instance}
     */
    private static String serviceName(ServiceInstance instance) {
        return instance.getServiceId().toUpperCase(Locale.ROOT);
    }

    private boolean isLocal(ServiceInstance instance) {
        if (ServiceInstanceKey.of(instance).equals(ServiceInstanceKey.of(localRegistration))) {
            return true;
        }
        URI localUri = endpointOf(localRegistration);
        return localUri != null && Objects.equals(endpointOf(instance), localUri);
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
        ConsistentHash current = ring;
        descriptor.describeProperty("localMember", localMember().name());
        descriptor.describeProperty("localCapabilities", localCapabilities.toString());
        descriptor.describeProperty("ringVersion", current.version());
        descriptor.describeProperty("members", memberNames(current.members()));
    }

    private static List<String> memberNames(Set<Member> members) {
        return members.stream().map(Member::name).sorted().toList();
    }
}
