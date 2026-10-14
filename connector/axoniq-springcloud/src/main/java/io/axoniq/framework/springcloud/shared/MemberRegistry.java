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

import io.axoniq.framework.springcloud.routing.ConsistentHash;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The administration of a member discovery: which members make up the cluster, what each of them handles, and which
 * member a command or query is routed to as a result.
 * <p>
 * The members come from two sources, which this registry combines into one {@link ConsistentHash} ring:
 * <ul>
 *     <li><em>This application</em>, whose capabilities are recorded through {@link #publishLocalCommands(int, Set)}
 *     and {@link #publishLocalQueries(Set)}. The command and the query side each record their own part, without
 *     erasing what the other recorded.</li>
 *     <li><em>The other members</em>, recorded through {@link #replaceRemoteMembers(Map, boolean)} and
 *     {@link #remove(Member)}.</li>
 * </ul>
 * Because the ring is derived from both every time either changes, a capability this application records is never
 * overwritten by a discovery update arriving at the same time, nor the other way around.
 * <p>
 * Only records what it is told. Finding the members, deciding when to look again, and deciding what to do with a
 * member that could not be reached is up to the {@link SpringCloudMemberDiscovery} owning it.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
class MemberRegistry implements DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(MemberRegistry.class);

    private final Member localMember;

    private volatile ConsistentHash ring = new ConsistentHash();
    // The other members, and whether this application is part of the ring, as last recorded. Guarded by this object's
    // monitor. Until anything is recorded, this application is the whole cluster it knows of.
    private Map<Member, MemberCapabilities> remoteMembers = Map.of();
    private boolean includesLocal = true;

    // The parts of this member's capabilities are held separately because each side only knows its own: recording the
    // whole record from either would erase what the other had recorded.
    private volatile int localLoadFactor = 0;
    private volatile Set<QualifiedName> localCommands = Set.of();
    private volatile Set<QualifiedName> localQueries = Set.of();

    private final AtomicInteger queryRotation = new AtomicInteger();
    private final List<Consumer<ConsistentHash>> membershipListeners = new CopyOnWriteArrayList<>();

    /**
     * Constructs a {@code MemberRegistry} for the application represented by the given {@code localMember}.
     *
     * @param localMember the member representing this application
     */
    MemberRegistry(Member localMember) {
        this.localMember = Objects.requireNonNull(localMember, "The localMember must not be null.");
    }

    /**
     * Resolves the member a command of the given {@code commandName} carrying the given {@code routingKey} is routed
     * to, on the ring.
     *
     * @param routingKey  the routing key of the command to resolve a member for
     * @param commandName the {@link QualifiedName} of the command to resolve a member for
     * @return the member that should handle the command, or {@link Optional#empty()} when none does
     */
    Optional<Member> findCommandDestination(String routingKey, QualifiedName commandName) {
        return ring.memberFor(routingKey, commandName);
    }

    /**
     * Resolves the member a query of the given {@code queryName} is routed to, rotating over every member advertising
     * it in name order.
     *
     * @param queryName the {@link QualifiedName} of the query to resolve a member for
     * @return the member that should handle the query, or {@link Optional#empty()} when none does
     */
    Optional<Member> findQueryDestination(QualifiedName queryName) {
        List<Member> candidates = findAllQueryDestinations(queryName);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        int index = Math.floorMod(queryRotation.getAndIncrement(), candidates.size());
        return Optional.of(candidates.get(index));
    }

    /**
     * Returns every member advertising the given {@code queryName}, in name order.
     *
     * @param queryName the {@link QualifiedName} of the query to resolve members for
     * @return every member advertising the given {@code queryName}, empty when none does
     */
    List<Member> findAllQueryDestinations(QualifiedName queryName) {
        return ring.queryHandlers(queryName);
    }

    /**
     * Registers a {@code listener} notified with the new ring after every change to it, outside the lock it is
     * replaced under, on the thread that made the change.
     *
     * @param listener notified with the new ring after every membership change
     * @return a registration cancelling the notification
     */
    org.axonframework.common.Registration onMembershipChanged(Consumer<ConsistentHash> listener) {
        Objects.requireNonNull(listener, "The listener must not be null.");
        membershipListeners.add(listener);
        return () -> membershipListeners.remove(listener);
    }

    /**
     * Records the given {@code commands} as the commands this application handles, asking for the given
     * {@code loadFactor} worth of the command load, and reflects them in the ring right away.
     *
     * @param loadFactor the share of the command load this application asks for
     * @param commands   the names of the commands this application handles
     * @return this application's capabilities as a whole, after recording
     */
    MemberCapabilities publishLocalCommands(int loadFactor, Set<QualifiedName> commands) {
        if (loadFactor < 0) {
            throw new IllegalArgumentException("The load factor cannot be negative, but was [" + loadFactor + "].");
        }
        Objects.requireNonNull(commands, "The commands must not be null.");
        this.localLoadFactor = loadFactor;
        this.localCommands = Set.copyOf(commands);
        rederiveRing();
        return localCapabilities();
    }

    /**
     * Records the given {@code queries} as the queries this application handles, and reflects them in the ring right
     * away.
     *
     * @param queries the names of the queries this application handles
     * @return this application's capabilities as a whole, after recording
     */
    MemberCapabilities publishLocalQueries(Set<QualifiedName> queries) {
        Objects.requireNonNull(queries, "The queries must not be null.");
        this.localQueries = Set.copyOf(queries);
        rederiveRing();
        return localCapabilities();
    }

    /**
     * Composes this application's capabilities from the parts each side recorded.
     *
     * @return the capabilities this application currently has
     */
    MemberCapabilities localCapabilities() {
        return new MemberCapabilities(localLoadFactor, localCommands, localQueries);
    }

    /**
     * Replaces the other members of the cluster with the given {@code members}.
     * <p>
     * This application is part of its own ring when {@code includesLocal} is {@code true}, under the capabilities it
     * recorded, rather than under whatever was learned of them through discovery.
     *
     * @param members       every other member of the cluster, and what each of them handles
     * @param includesLocal whether this application is part of the ring
     */
    void replaceRemoteMembers(Map<Member, MemberCapabilities> members, boolean includesLocal) {
        Objects.requireNonNull(members, "The members must not be null.");
        synchronized (this) {
            this.remoteMembers = Map.copyOf(members);
            this.includesLocal = includesLocal;
        }
        rederiveRing();
    }

    /**
     * Takes the given {@code member} off the ring until it is part of the members recorded next.
     *
     * @param member the member to take off the ring
     */
    void remove(Member member) {
        Objects.requireNonNull(member, "The member must not be null.");
        synchronized (this) {
            Map<Member, MemberCapabilities> remaining = new LinkedHashMap<>(remoteMembers);
            if (remaining.keySet().removeIf(candidate -> candidate.name().equals(member.name()))) {
                logger.info("Removing member [{}] from the ring.", member);
                remoteMembers = remaining;
            }
        }
        rederiveRing();
    }

    /**
     * Derives the ring from the other members and this application's own capabilities, and notifies the membership
     * listeners when it changed.
     * <p>
     * Deriving it under one lock from everything known, rather than updating the ring from whichever part changed,
     * keeps a recorded capability and a discovery update arriving at once from overwriting each other.
     */
    private void rederiveRing() {
        ConsistentHash previous;
        ConsistentHash updated;
        synchronized (this) {
            Map<Member, MemberCapabilities> memberships = new LinkedHashMap<>(remoteMembers);
            if (includesLocal) {
                memberships.put(localMember, localCapabilities());
            }
            previous = ring;
            updated = ring = previous.withOnly(memberships);
        }
        if (updated != previous) {
            logger.debug("The ring is now [{}].", updated);
            notifyMembershipChanged(updated);
        }
    }

    /**
     * Notifies the membership listeners of the given {@code current} ring.
     * <p>
     * A listener that throws must not stop the others from being told, nor break the change that led here.
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

    /**
     * Returns the {@link Member} representing this application.
     *
     * @return the member representing this application
     */
    Member localMember() {
        return localMember;
    }

    /**
     * Returns the ring currently routed with.
     *
     * @return the current routing ring
     */
    ConsistentHash ring() {
        return ring;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        ConsistentHash current = ring;
        descriptor.describeProperty("localMember", localMember.name());
        descriptor.describeProperty("localCapabilities", localCapabilities().toString());
        descriptor.describeProperty("ringVersion", current.version());
        descriptor.describeProperty("members", current.members().stream().map(Member::name).sorted().toList());
    }
}
