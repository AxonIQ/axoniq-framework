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

import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the routing behaviour of the {@link ConsistentHash} ring.
 *
 * @author Allard Buijze
 */
class ConsistentHashTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final QualifiedName SUBSCRIBE_STUDENT = new QualifiedName("university.SubscribeStudent");

    private static Member member(String name) {
        return new Member(name, URI.create("http://" + name + ":8080"), false);
    }

    private static MemberCapabilities handling(int loadFactor, QualifiedName... commands) {
        return new MemberCapabilities(loadFactor, Set.of(commands), Set.of());
    }

    @Nested
    class Resolving {

        @Test
        void resolvesNoMemberOnAnEmptyRing() {
            // given
            ConsistentHash ring = new ConsistentHash();

            // when
            Optional<Member> resolved = ring.getMember("course-1", CREATE_COURSE);

            // then
            assertThat(resolved).isEmpty();
        }

        @Test
        void resolvesTheOnlyMemberHandlingTheCommand() {
            // given
            Member only = member("node-a");
            ConsistentHash ring = new ConsistentHash().with(only, handling(100, CREATE_COURSE));

            // when
            Optional<Member> resolved = ring.getMember("course-1", CREATE_COURSE);

            // then
            assertThat(resolved).contains(only);
        }

        @Test
        void resolvesNoMemberWhenNoneHandlesTheCommand() {
            // given — both members are on the ring, but neither subscribed to the command being routed
            ConsistentHash ring = new ConsistentHash()
                    .with(member("node-a"), handling(100, CREATE_COURSE))
                    .with(member("node-b"), handling(100, RENAME_COURSE));

            // when
            Optional<Member> resolved = ring.getMember("course-1", SUBSCRIBE_STUDENT);

            // then
            assertThat(resolved).isEmpty();
        }

        @Test
        void resolvesOnlyMembersHandlingTheCommandEvenWhenOthersOwnTheRingPosition() {
            // given — node-b handles nothing that is routed here, so every routing key must land on node-a
            Member handler = member("node-a");
            ConsistentHash ring = new ConsistentHash()
                    .with(handler, handling(100, CREATE_COURSE))
                    .with(member("node-b"), handling(100, RENAME_COURSE));

            // when / then — sampling many keys proves the filter holds all the way around the ring
            assertThat(IntStream.range(0, 500)
                                .mapToObj(i -> ring.getMember("course-" + i, CREATE_COURSE))
                                .toList())
                    .allSatisfy(resolved -> assertThat(resolved).contains(handler));
        }

        @Test
        void resolvesTheSameMemberForTheSameRoutingKey() {
            // given
            ConsistentHash ring = new ConsistentHash()
                    .with(member("node-a"), handling(100, CREATE_COURSE))
                    .with(member("node-b"), handling(100, CREATE_COURSE))
                    .with(member("node-c"), handling(100, CREATE_COURSE));

            // when
            Optional<Member> first = ring.getMember("course-1", CREATE_COURSE);
            Optional<Member> second = ring.getMember("course-1", CREATE_COURSE);

            // then
            assertThat(first).isPresent().isEqualTo(second);
        }

        @Test
        void spreadsRoutingKeysAcrossEveryMemberHandlingTheCommand() {
            // given
            ConsistentHash ring = new ConsistentHash()
                    .with(member("node-a"), handling(100, CREATE_COURSE))
                    .with(member("node-b"), handling(100, CREATE_COURSE))
                    .with(member("node-c"), handling(100, CREATE_COURSE));

            // when
            Set<String> selected = IntStream.range(0, 500)
                                            .mapToObj(i -> ring.getMember("course-" + i, CREATE_COURSE))
                                            .flatMap(Optional::stream)
                                            .map(Member::name)
                                            .collect(Collectors.toSet());

            // then — every member takes a share; a ring that funnelled everything to one node would be useless
            assertThat(selected).containsExactlyInAnyOrder("node-a", "node-b", "node-c");
        }

        @Test
        void keepsMostRoutingKeysOnTheSameMemberWhenAnotherJoins() {
            // given
            ConsistentHash before = new ConsistentHash()
                    .with(member("node-a"), handling(100, CREATE_COURSE))
                    .with(member("node-b"), handling(100, CREATE_COURSE));

            // when — a third member joins
            ConsistentHash after = before.with(member("node-c"), handling(100, CREATE_COURSE));

            // then — the point of consistent hashing: adding a third member moves roughly a third of the keys,
            // not all of them, so most entities stay put
            long moved = IntStream.range(0, 600)
                                  .filter(i -> !before.getMember("course-" + i, CREATE_COURSE)
                                                      .equals(after.getMember("course-" + i, CREATE_COURSE)))
                                  .count();
            assertThat(moved).isLessThan(300);
        }
    }

    @Nested
    class LoadFactor {

        @Test
        void neverResolvesToAMemberWithoutRingPositions() {
            // given — node-b asks for no load at all
            Member loaded = member("node-a");
            ConsistentHash ring = new ConsistentHash()
                    .with(loaded, handling(100, CREATE_COURSE))
                    .with(member("node-b"), handling(0, CREATE_COURSE));

            // when / then
            assertThat(IntStream.range(0, 300)
                                .mapToObj(i -> ring.getMember("course-" + i, CREATE_COURSE))
                                .toList())
                    .allSatisfy(resolved -> assertThat(resolved).contains(loaded));
        }

        @Test
        void keepsAMemberWithoutRingPositionsAsAMember() {
            // given
            Member incapable = member("node-b");

            // when
            ConsistentHash ring = new ConsistentHash().with(incapable, MemberCapabilities.INCAPABLE);

            // then — it stays known, so a later heartbeat can give it capabilities without treating it as new
            assertThat(ring.getMembers()).containsExactly(incapable);
            assertThat(ring.capabilitiesOf(incapable)).contains(MemberCapabilities.INCAPABLE);
        }

        @Test
        void favoursTheMemberAskingForMoreLoad() {
            // given — node-a asks for nine times the load of node-b
            ConsistentHash ring = new ConsistentHash()
                    .with(member("node-a"), handling(900, CREATE_COURSE))
                    .with(member("node-b"), handling(100, CREATE_COURSE));

            // when
            long onNodeA = IntStream.range(0, 1000)
                                    .mapToObj(i -> ring.getMember("course-" + i, CREATE_COURSE))
                                    .flatMap(Optional::stream)
                                    .filter(resolved -> resolved.name().equals("node-a"))
                                    .count();

            // then — the split follows the load factors rather than member count
            assertThat(onNodeA).isGreaterThan(700);
        }
    }

    @Nested
    class Membership {

        @Test
        void returnsTheSameRingWhenRegisteringAnUnchangedMember() {
            // given
            Member node = member("node-a");
            ConsistentHash ring = new ConsistentHash().with(node, handling(100, CREATE_COURSE));

            // when
            ConsistentHash unchanged = ring.with(node, handling(100, CREATE_COURSE));

            // then — no change means no new ring, so readers need not be disturbed
            assertThat(unchanged).isSameAs(ring);
            assertThat(unchanged.version()).isEqualTo(ring.version());
        }

        @Test
        void replacesTheRegistrationOfAMemberWithTheSameName() {
            // given
            Member node = member("node-a");
            ConsistentHash ring = new ConsistentHash().with(node, handling(100, CREATE_COURSE));

            // when
            ConsistentHash updated = ring.with(node, handling(100, CREATE_COURSE, RENAME_COURSE));

            // then
            assertThat(updated.getMembers()).containsExactly(node);
            assertThat(updated.getMember("course-1", RENAME_COURSE)).contains(node);
            assertThat(updated.version()).isEqualTo(ring.version() + 1);
        }

        @Test
        void leavesTheOriginalRingUntouchedWhenRegisteringAMember() {
            // given
            ConsistentHash ring = new ConsistentHash().with(member("node-a"), handling(100, CREATE_COURSE));

            // when
            ring.with(member("node-b"), handling(100, CREATE_COURSE));

            // then — the ring is immutable, which is what lets it be published without synchronisation
            assertThat(ring.getMembers()).extracting(Member::name).containsExactly("node-a");
        }

        @Test
        void removesAMember() {
            // given
            Member removed = member("node-b");
            ConsistentHash ring = new ConsistentHash()
                    .with(member("node-a"), handling(100, CREATE_COURSE))
                    .with(removed, handling(100, CREATE_COURSE));

            // when
            ConsistentHash without = ring.without(removed);

            // then
            assertThat(without.getMembers()).extracting(Member::name).containsExactly("node-a");
            assertThat(without.version()).isEqualTo(ring.version() + 1);
        }

        @Test
        void returnsTheSameRingWhenRemovingAnUnknownMember() {
            // given
            ConsistentHash ring = new ConsistentHash().with(member("node-a"), handling(100, CREATE_COURSE));

            // when
            ConsistentHash unchanged = ring.without(member("node-z"));

            // then
            assertThat(unchanged).isSameAs(ring);
        }

        @Test
        void reportsNoCapabilitiesForAnUnknownMember() {
            // given
            ConsistentHash ring = new ConsistentHash().with(member("node-a"), handling(100, CREATE_COURSE));

            // when / then
            assertThat(ring.capabilitiesOf(member("node-z"))).isEmpty();
        }
    }

    @Nested
    class Agreement {

        @Test
        void resolvesIdenticallyRegardlessOfTheOrderMembersWereRegisteredIn() {
            // given — two members building the same cluster view in different orders
            Member a = member("node-a");
            Member b = member("node-b");
            Member c = member("node-c");
            ConsistentHash oneOrder = new ConsistentHash().with(a, handling(100, CREATE_COURSE))
                                                          .with(b, handling(100, CREATE_COURSE))
                                                          .with(c, handling(100, CREATE_COURSE));
            ConsistentHash otherOrder = new ConsistentHash().with(c, handling(100, CREATE_COURSE))
                                                            .with(a, handling(100, CREATE_COURSE))
                                                            .with(b, handling(100, CREATE_COURSE));

            // when / then — if two members disagreed on where a key belongs, commands for one entity would be
            // handled on two nodes at once
            assertThat(oneOrder).isEqualTo(otherOrder);
            assertThat(IntStream.range(0, 300).allMatch(
                    i -> oneOrder.getMember("course-" + i, CREATE_COURSE)
                                 .equals(otherOrder.getMember("course-" + i, CREATE_COURSE))
            )).isTrue();
        }

        @Test
        void appliesTheConfiguredHashFunctionToRoutingKeysAndRingPositions() {
            // given — a hash function collapsing everything onto one position; if routing keys were hashed with a
            // different function than ring positions, no member would ever be found
            ConsistentHash ring = new ConsistentHash(key -> "fixed")
                    .with(member("node-a"), handling(100, CREATE_COURSE));

            // when / then
            assertThat(ring.getMember("course-1", CREATE_COURSE)).isPresent();
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullMember() {
            // given
            ConsistentHash ring = new ConsistentHash();

            // when / then
            assertThatThrownBy(() -> ring.with(null, MemberCapabilities.INCAPABLE))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> ring.without(null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANullHashFunction() {
            // when / then
            assertThatThrownBy(() -> new ConsistentHash(null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANullRoutingKeyOrCommandName() {
            // given
            ConsistentHash ring = new ConsistentHash();

            // when / then
            assertThatThrownBy(() -> ring.getMember(null, CREATE_COURSE)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> ring.getMember("course-1", null)).isInstanceOf(NullPointerException.class);
        }
    }
}
