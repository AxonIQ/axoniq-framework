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
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link MemberRegistry} combines this application's capabilities and the other members it is given into
 * one ring, and routes with it.
 *
 * @author Allard Buijze
 */
class MemberRegistryTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final QualifiedName FIND_COURSE = new QualifiedName("university.FindCourse");
    private static final MemberCapabilities HANDLES_CREATE =
            new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of());
    private static final MemberCapabilities HANDLES_FIND =
            new MemberCapabilities(100, Set.of(), Set.of(FIND_COURSE));
    private static final Member REMOTE = new Member("node-b", URI.create("http://node-b:8080"), false);

    private MemberRegistry testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new MemberRegistry(Member.localMember("node-a"));
    }

    @Nested
    class ReplacingRemoteMembers {

        @Test
        void routesToTheMembersItIsGiven() {
            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // then
            assertThat(testSubject.findCommandDestination("course-1", CREATE_COURSE)).contains(REMOTE);
        }

        @Test
        void dropsMembersThatAreNoLongerGiven() {
            // given
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // when
            testSubject.replaceRemoteMembers(Map.of(), true);

            // then
            assertThat(testSubject.ring().members()).doesNotContain(REMOTE);
        }

        @Test
        void leavesThisApplicationOutWhenToldItIsNotPartOfTheRing() {
            // given
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), false);

            // then
            assertThat(testSubject.ring().members()).containsExactly(REMOTE);
        }

        @Test
        void keepsLeavingThisApplicationOutWhenItPublishesNewCapabilities() {
            // given
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), false);

            // when
            testSubject.publishLocalCommands(100, Set.of(RENAME_COURSE));

            // then — the other members do not route to this application, so neither does it
            assertThat(testSubject.findCommandDestination("course-1", RENAME_COURSE)).isEmpty();
        }

        @Test
        void keepsThisApplicationsPublishedCapabilities() {
            // given
            testSubject.publishLocalCommands(100, Set.of(RENAME_COURSE));

            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // then
            assertThat(testSubject.findCommandDestination("course-1", RENAME_COURSE))
                    .contains(testSubject.localMember());
        }

        @Test
        void notifiesMembershipListenersOfAChangedRing() {
            // given
            List<ConsistentHash> notified = new ArrayList<>();
            testSubject.onMembershipChanged(notified::add);

            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // then
            assertThat(notified).containsExactly(testSubject.ring());
        }

        @Test
        void doesNotNotifyMembershipListenersWhenNothingChanged() {
            // given
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);
            List<ConsistentHash> notified = new ArrayList<>();
            testSubject.onMembershipChanged(notified::add);

            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // then
            assertThat(notified).isEmpty();
        }

        @Test
        void keepsNotifyingOtherListenersWhenOneFails() {
            // given
            List<ConsistentHash> notified = new ArrayList<>();
            testSubject.onMembershipChanged(ring -> {
                throw new IllegalStateException("listener failure");
            });
            testSubject.onMembershipChanged(notified::add);

            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // then
            assertThat(notified).hasSize(1);
        }

        @Test
        void stopsNotifyingACancelledListener() {
            // given
            List<ConsistentHash> notified = new ArrayList<>();
            testSubject.onMembershipChanged(notified::add).cancel();

            // when
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // then
            assertThat(notified).isEmpty();
        }
    }

    @Nested
    class RoutingQueries {

        @Test
        void findsNoDestinationWhenNoMemberHandlesTheQuery() {
            // given a cluster whose members handle commands only
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // when / then
            assertThat(testSubject.findQueryDestination(FIND_COURSE)).isEmpty();
        }

        @Test
        void routesToTheOnlyMemberHandlingTheQuery() {
            // given
            testSubject.publishLocalQueries(Set.of(FIND_COURSE));

            // when
            Optional<Member> destination = testSubject.findQueryDestination(FIND_COURSE);

            // then
            assertThat(destination).isPresent();
            assertThat(destination.get().local()).isTrue();
        }

        @Test
        void rotatesOverEveryMemberHandlingTheQuery() {
            // given two members both handling the query
            testSubject.publishLocalQueries(Set.of(FIND_COURSE));
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_FIND), true);

            // when the same query name is routed as many times as there are members
            List<String> destinations = List.of(testSubject.findQueryDestination(FIND_COURSE).orElseThrow().name(),
                                                testSubject.findQueryDestination(FIND_COURSE).orElseThrow().name());

            // then the load is spread rather than always landing on the same member
            assertThat(destinations).doesNotHaveDuplicates().hasSize(2);
        }

        @Test
        void skipsMembersThatDoNotHandleTheQuery() {
            // given only the remote member handling the query
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_FIND), true);

            // when routed repeatedly, so that a rotation would reach a non-handling member if it included one
            List<Optional<Member>> destinations = List.of(testSubject.findQueryDestination(FIND_COURSE),
                                                          testSubject.findQueryDestination(FIND_COURSE),
                                                          testSubject.findQueryDestination(FIND_COURSE));

            // then every query went to the one member advertising the name
            assertThat(destinations).allSatisfy(destination -> {
                assertThat(destination).isPresent();
                assertThat(destination.get().local()).isFalse();
            });
        }

        @Test
        void rejectsANullQueryName() {
            // when / then
            assertThatThrownBy(() -> testSubject.findQueryDestination(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class LocalCapabilities {

        @Test
        void makesThisApplicationRoutableImmediately() {
            // when — a command may be dispatched right after its handler subscribed, before any heartbeat
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // then
            Optional<Member> destination = testSubject.findCommandDestination("course-1", CREATE_COURSE);
            assertThat(destination).isPresent();
            assertThat(destination.get().local()).isTrue();
        }

        @Test
        void replacesWhatWasPublishedBefore() {
            // given
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // when
            testSubject.publishLocalCommands(100, Set.of(RENAME_COURSE));

            // then
            assertThat(testSubject.findCommandDestination("course-1", CREATE_COURSE)).isEmpty();
            assertThat(testSubject.findCommandDestination("course-1", RENAME_COURSE)).isPresent();
        }

        @Test
        void rejectsNullCommands() {
            // when / then
            assertThatThrownBy(() -> testSubject.publishLocalCommands(100, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANegativeLoadFactor() {
            // when / then
            assertThatThrownBy(() -> testSubject.publishLocalCommands(-1, Set.of(CREATE_COURSE)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("load factor");
        }

        @Test
        void keepsPublishedQueriesWhenCommandsArePublished() {
            // given a member handling both, as an application with a query handler and a command handler has
            testSubject.publishLocalQueries(Set.of(FIND_COURSE));

            // when
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // then neither publication erased the other
            assertThat(testSubject.localCapabilities().commands()).containsExactly(CREATE_COURSE);
            assertThat(testSubject.localCapabilities().queries()).containsExactly(FIND_COURSE);
        }

        @Test
        void keepsPublishedCommandsWhenQueriesArePublished() {
            // given
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // when
            testSubject.publishLocalQueries(Set.of(FIND_COURSE));

            // then
            assertThat(testSubject.localCapabilities().commands()).containsExactly(CREATE_COURSE);
            assertThat(testSubject.localCapabilities().queries()).containsExactly(FIND_COURSE);
        }

        @Test
        void rejectsNullQueries() {
            // when / then
            assertThatThrownBy(() -> testSubject.publishLocalQueries(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class RemovingMembers {

        @Test
        void takesTheMemberOffTheRing() {
            // given
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);

            // when
            testSubject.remove(REMOTE);

            // then
            assertThat(testSubject.ring().members()).doesNotContain(REMOTE);
        }

        @Test
        void ignoresAMemberThatIsNotOnTheRing() {
            // given
            testSubject.replaceRemoteMembers(Map.of(REMOTE, HANDLES_CREATE), true);
            int versionBefore = testSubject.ring().version();

            // when
            testSubject.remove(new Member("unknown-node", URI.create("http://node-z:8080"), false));

            // then
            assertThat(testSubject.ring().version()).isEqualTo(versionBefore);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullLocalMember() {
            // when / then
            assertThatThrownBy(() -> new MemberRegistry(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
