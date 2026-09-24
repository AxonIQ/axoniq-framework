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

package io.axoniq.framework.springcloud.routing;

import org.axonframework.common.digest.Digester;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An immutable consistent-hash ring resolving a routing key to the {@link Member} that should handle a command.
 * <p>
 * Each member claims a number of positions on the ring proportional to its
 * {@link MemberCapabilities#loadFactor() load factor}. Resolving a routing key hashes it onto the ring and walks
 * clockwise until a member is found that {@link MemberCapabilities#handlesCommand(QualifiedName) handles} the command
 * in question. The same routing key therefore always resolves to the same member for as long as the memberships do
 * not change, which is what keeps commands for one entity on one node.
 * <p>
 * Instances are immutable: {@link #with(Member, MemberCapabilities)} and {@link #without(Member)} return a new ring
 * rather than mutating this one, so a ring can be published to readers without further synchronisation. Every change
 * increases the {@link #version()}.
 * <p>
 * A member with a load factor of {@code 0} — including one whose capabilities could not be retrieved, reported as
 * {@link MemberCapabilities#INCAPABLE} — claims no positions and is never resolved to, but remains part of
 * {@link #members()}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public final class ConsistentHash {

    private final Map<String, RingMember> members;
    private final SortedMap<String, RingMember> hashToMember;
    private final UnaryOperator<String> hashFunction;
    private final int version;

    /**
     * Constructs an empty {@code ConsistentHash} hashing routing keys and ring positions with an MD5 digest.
     * <p>
     * Register members through {@link #with(Member, MemberCapabilities)}.
     */
    public ConsistentHash() {
        this(Digester::md5Hex);
    }

    /**
     * Constructs an empty {@code ConsistentHash} using the given {@code hashFunction} to hash both routing keys and
     * the ring positions claimed by each member.
     * <p>
     * Every member of the cluster must use the same hash function, or they will disagree on where commands belong.
     * Register members through {@link #with(Member, MemberCapabilities)}.
     *
     * @param hashFunction the function hashing a routing key, or a member's ring position, onto the ring. Must be
     *                     deterministic, and must order its output consistently for all members of the cluster.
     */
    public ConsistentHash(UnaryOperator<String> hashFunction) {
        this.hashFunction = Objects.requireNonNull(hashFunction, "The hashFunction must not be null.");
        this.members = Collections.emptyMap();
        this.hashToMember = Collections.emptySortedMap();
        this.version = 0;
    }

    private ConsistentHash(Map<String, RingMember> members, UnaryOperator<String> hashFunction, int version) {
        this.hashFunction = hashFunction;
        this.members = members;
        this.version = version;
        SortedMap<String, RingMember> positions = new TreeMap<>();
        // Iterating a name-sorted map keeps position assignment identical on every member of the cluster, so two
        // members hashing to the same ring position resolve the same way everywhere.
        members.values().forEach(member -> member.hashes().forEach(hash -> positions.put(hash, member)));
        this.hashToMember = Collections.unmodifiableSortedMap(positions);
    }

    /**
     * Returns a {@code ConsistentHash} holding exactly the given {@code memberships}, hashed with this ring's hash
     * function.
     * <p>
     * Intended for rebuilding a ring from scratch, as a discovery round does. Registering members one by one through
     * {@link #with(Member, MemberCapabilities)} would recompute every member's ring positions on each call, making a
     * rebuild quadratic in the number of members; this assigns the positions once.
     * <p>
     * Members this ring holds that are absent from {@code memberships} are not carried over — the result describes the
     * cluster as {@code memberships} describes it, which is what makes it a rebuild rather than an update.
     *
     * @param memberships the members to register, and the messages each of them handles
     * @return a {@code ConsistentHash} holding exactly the given {@code memberships}, or {@code this} when it already
     * holds exactly those memberships
     */
    public ConsistentHash withOnly(Map<Member, MemberCapabilities> memberships) {
        Objects.requireNonNull(memberships, "The memberships must not be null.");

        Map<String, RingMember> rebuilt = new TreeMap<>();
        memberships.forEach((member, capabilities) -> rebuilt.put(
                member.name(), new RingMember(member, capabilities, hashFunction)
        ));
        if (rebuilt.equals(members)) {
            return this;
        }
        return new ConsistentHash(rebuilt, hashFunction, version + 1);
    }

    /**
     * Returns a {@code ConsistentHash} with the given {@code member} registered under the given
     * {@code capabilities}, replacing any earlier registration of a member with the same {@link Member#name() name}.
     *
     * @param member       the member to register
     * @param capabilities the messages the given {@code member} handles, and the command load it asks for
     * @return a {@code ConsistentHash} including the given {@code member}, or {@code this} when the given
     * {@code member} was already registered with identical {@code capabilities}
     */
    public ConsistentHash with(Member member, MemberCapabilities capabilities) {
        Objects.requireNonNull(member, "The member must not be null.");
        Objects.requireNonNull(capabilities, "The capabilities must not be null.");

        RingMember updated = new RingMember(member, capabilities, hashFunction);
        if (updated.equals(members.get(member.name()))) {
            return this;
        }

        Map<String, RingMember> updatedMembers = new TreeMap<>(members);
        updatedMembers.put(member.name(), updated);
        return new ConsistentHash(updatedMembers, hashFunction, version + 1);
    }

    /**
     * Returns a {@code ConsistentHash} without the given {@code member}.
     *
     * @param member the member to deregister
     * @return a {@code ConsistentHash} excluding the given {@code member}, or {@code this} when the given
     * {@code member} was not registered
     */
    public ConsistentHash without(Member member) {
        Objects.requireNonNull(member, "The member must not be null.");
        if (!members.containsKey(member.name())) {
            return this;
        }

        Map<String, RingMember> updatedMembers = new TreeMap<>(members);
        updatedMembers.remove(member.name());
        return new ConsistentHash(updatedMembers, hashFunction, version + 1);
    }

    /**
     * Resolves the {@link Member} that should handle a command of the given {@code commandName} carrying the given
     * {@code routingKey}.
     *
     * @param routingKey  the routing key of the command to resolve a member for
     * @param commandName the {@link QualifiedName} of the command to resolve a member for. Only members that
     *                    subscribed to this name are considered.
     * @return the member that should handle the command, or {@link Optional#empty()} when no registered member
     * handles commands of the given {@code commandName}
     */
    public Optional<Member> memberFor(String routingKey, QualifiedName commandName) {
        Objects.requireNonNull(routingKey, "The routingKey must not be null.");
        Objects.requireNonNull(commandName, "The commandName must not be null.");

        String hash = hashFunction.apply(routingKey);
        // Walking the tail first and wrapping around to the head is what makes the ring a ring: the first suitable
        // member clockwise from the routing key's position handles it.
        return firstHandlerOf(commandName, hashToMember.tailMap(hash).values())
                .or(() -> firstHandlerOf(commandName, hashToMember.headMap(hash).values()));
    }

    private static Optional<Member> firstHandlerOf(QualifiedName commandName, Collection<RingMember> candidates) {
        return candidates.stream()
                         .filter(candidate -> candidate.capabilities().handlesCommand(commandName))
                         .map(RingMember::member)
                         .findFirst();
    }

    /**
     * Returns every {@link Member} registered with this ring, including those claiming no ring positions.
     *
     * @return every registered member, in no particular order
     */
    public Set<Member> members() {
        return members.values().stream().map(RingMember::member).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns the {@link MemberCapabilities} the given {@code member} is registered under.
     *
     * @param member the member to look up the capabilities of
     * @return the capabilities of the given {@code member}, or {@link Optional#empty()} when it is not registered
     */
    public Optional<MemberCapabilities> capabilitiesOf(Member member) {
        Objects.requireNonNull(member, "The member must not be null.");
        return Optional.ofNullable(members.get(member.name())).map(RingMember::capabilities);
    }

    /**
     * Returns the version of this ring, increased by one on every change in memberships.
     * <p>
     * Two rings with the same version are not necessarily equal — the version counts changes made along one lineage
     * of rings, and says nothing about rings built independently.
     *
     * @return the number of changes made to reach this ring
     */
    public int version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsistentHash that)) {
            return false;
        }
        return Objects.equals(members, that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(members);
    }

    @Override
    public String toString() {
        return members.values().stream()
                      .map(RingMember::toString)
                      .collect(Collectors.joining(", ", "ConsistentHash[", "]"));
    }

    /**
     * A {@link Member} as registered with a {@link ConsistentHash}, paired with its {@link MemberCapabilities} and
     * the ring positions it claims.
     * <p>
     * The positions are computed once, on construction, because a ring is rebuilt on every discovery heartbeat and
     * recomputing a hash per position per lookup would dominate the cost of routing a command.
     *
     * @param member       the member registered with the ring
     * @param capabilities the messages the {@code member} handles, and the command load it asks for
     * @param hashes       the positions the {@code member} claims on the ring
     */
    private record RingMember(Member member, MemberCapabilities capabilities, Set<String> hashes) {

        private RingMember(Member member, MemberCapabilities capabilities, Function<String, String> hashFunction) {
            this(member, capabilities, positionsOf(member, capabilities.loadFactor(), hashFunction));
        }

        private static Set<String> positionsOf(Member member, int loadFactor, Function<String, String> hashFunction) {
            return IntStream.range(0, loadFactor)
                            .mapToObj(position -> hashFunction.apply(member.name() + " #" + position))
                            .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        @Override
        public String toString() {
            return member.name() + "(" + capabilities.loadFactor() + ")";
        }
    }
}
