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

import io.axoniq.framework.springcloud.discovery.RecordingCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.ServiceInstanceKey;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import io.axoniq.framework.springcloud.util.RecordingDiscoveryClient;
import io.axoniq.framework.springcloud.util.TestServiceInstance;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link SpringCloudMemberRegistry} builds and maintains the routing ring from what discovery reports.
 *
 * @author Allard Buijze
 */
class SpringCloudMemberRegistryTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final QualifiedName FIND_COURSE = new QualifiedName("university.FindCourse");
    private static final MemberCapabilities HANDLES_CREATE =
            new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of());
    private static final MemberCapabilities HANDLES_RENAME =
            new MemberCapabilities(100, Set.of(RENAME_COURSE), Set.of());

    private TestServiceInstance localInstance;
    private TestServiceInstance remoteInstance;
    private RecordingDiscoveryClient discoveryClient;
    private RecordingCapabilityDiscoveryMode discoveryMode;
    private SpringCloudMemberRegistry testSubject;

    @BeforeEach
    void setUp() {
        localInstance = TestServiceInstance.instance("university", "node-a", 8080);
        remoteInstance = TestServiceInstance.instance("university", "node-b", 8080);
        discoveryClient = new RecordingDiscoveryClient().register("university", localInstance, remoteInstance);
        discoveryMode = new RecordingCapabilityDiscoveryMode()
                .answering(localInstance, HANDLES_CREATE)
                .answering(remoteInstance, HANDLES_CREATE);
        testSubject = new SpringCloudMemberRegistry(discoveryClient, localInstance, discoveryMode);
    }

    @Nested
    class BuildingTheRing {

        @Test
        void publishesEmptyLocalCapabilitiesOnConstruction() {
            // then — the discovery mode must recognise this application's own instance from the first round, rather
            // than asking it for its capabilities over HTTP
            assertThat(discoveryMode.localInstance()).isEqualTo(localInstance);
            assertThat(discoveryMode.localCapabilities()).isEqualTo(MemberCapabilities.INCAPABLE);
        }

        @Test
        void includesEveryDiscoveredInstanceThatReportsCapabilities() {
            // when
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().getMembers()).hasSize(2);
        }

        @Test
        void marksThisApplicationsOwnInstanceAsLocal() {
            // given
            testSubject.onInstanceRegistered(new InstanceRegisteredEvent<>(this, localInstance));

            // when
            Set<Member> members = testSubject.ring().getMembers();

            // then — the local flag is what decides whether a command is handled here or sent over HTTP
            assertThat(members).filteredOn(Member::local).hasSize(1);
            assertThat(members).filteredOn(member -> !member.local()).hasSize(1);
        }

        @Test
        void leavesOutInstancesReportedAsNotPartOfTheCluster() {
            // given
            discoveryMode.reportingUnknown(remoteInstance);

            // when
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().getMembers()).hasSize(1);
        }

        @Test
        void leavesOutInstancesWhoseCapabilitiesCouldNotBeDiscovered() {
            // given
            discoveryMode.failingWithClientError(remoteInstance);

            // when
            testSubject.updateMemberships();

            // then — the round must not be abandoned because one instance threw
            assertThat(testSubject.ring().getMembers()).hasSize(1);
        }

        @Test
        void keepsThisApplicationInItsOwnRingWhenDiscoveryReportsNothing() {
            // given — discovery may not have anything to report yet
            discoveryClient.deregisterAll();
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // when
            testSubject.updateMemberships();

            // then — falling out of its own ring would leave an application unable to handle its own commands
            assertThat(testSubject.findCommandDestination("course-1", CREATE_COURSE)).isPresent();
        }

        @Test
        void tellsTheDiscoveryModeWhichInstancesRemain() {
            // when
            testSubject.updateMemberships();

            // then — the mode discards what it cached for instances that have gone away
            assertThat(discoveryMode.retained()).last()
                    .isEqualTo(Set.of(ServiceInstanceKey.of(localInstance),
                                      ServiceInstanceKey.of(remoteInstance)));
        }

        @Test
        void rebuildsTheRingOnEachHeartbeat() {
            // given
            testSubject.onHeartbeat(new HeartbeatEvent(this, "first"));
            assertThat(testSubject.ring().getMembers()).hasSize(2);

            // when — a member leaves
            discoveryClient.deregister("university", remoteInstance);
            testSubject.onHeartbeat(new HeartbeatEvent(this, "second"));

            // then — rebuilding means a departed member simply does not reappear
            assertThat(testSubject.ring().getMembers()).hasSize(1);
        }

        @Test
        void picksUpCapabilitiesAMemberGainedSinceTheLastRound() {
            // given
            testSubject.updateMemberships();
            assertThat(testSubject.ring().getMember("course-1", RENAME_COURSE)).isEmpty();

            // when
            discoveryMode.answering(remoteInstance,
                                    new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of()));
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().getMember("course-1", RENAME_COURSE)).isPresent();
        }
    }

    @Nested
    class RoutingQueries {

        @Test
        void findsNoDestinationWhenNoMemberHandlesTheQuery() {
            // given a cluster whose members handle commands only
            testSubject.updateMemberships();

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
            MemberCapabilities handlesFind = new MemberCapabilities(100, Set.of(), Set.of(FIND_COURSE));
            discoveryMode.answering(localInstance, handlesFind).answering(remoteInstance, handlesFind);
            testSubject.updateMemberships();

            // when the same query name is routed as many times as there are members
            List<String> destinations = List.of(testSubject.findQueryDestination(FIND_COURSE).orElseThrow().name(),
                                                testSubject.findQueryDestination(FIND_COURSE).orElseThrow().name());

            // then the load is spread rather than always landing on the same member
            assertThat(destinations).doesNotHaveDuplicates().hasSize(2);
        }

        @Test
        void skipsMembersThatDoNotHandleTheQuery() {
            // given only the remote member handling the query
            discoveryMode.answering(remoteInstance, new MemberCapabilities(100, Set.of(), Set.of(FIND_COURSE)));
            testSubject.updateMemberships();

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
        void publishesThemToTheDiscoveryMode() {
            // when
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // then — this is what other members read from the capabilities endpoint
            assertThat(discoveryMode.localCapabilities()).isEqualTo(HANDLES_CREATE);
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
            assertThat(discoveryMode.localCapabilities().commands()).containsExactly(CREATE_COURSE);
            assertThat(discoveryMode.localCapabilities().queries()).containsExactly(FIND_COURSE);
        }

        @Test
        void keepsPublishedCommandsWhenQueriesArePublished() {
            // given
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE));

            // when
            testSubject.publishLocalQueries(Set.of(FIND_COURSE));

            // then
            assertThat(discoveryMode.localCapabilities().commands()).containsExactly(CREATE_COURSE);
            assertThat(discoveryMode.localCapabilities().queries()).containsExactly(FIND_COURSE);
        }

        @Test
        void rejectsNullQueries() {
            // when / then
            assertThatThrownBy(() -> testSubject.publishLocalQueries(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Registration {

        @Test
        void namesThisApplicationProvisionallyUntilItHasRegistered() {
            // given — several discovery implementations throw when asked for a URI before registration completes
            TestServiceInstance unregistered = TestServiceInstance.withoutUri("university");
            SpringCloudMemberRegistry beforeRegistration = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient(), unregistered, new RecordingCapabilityDiscoveryMode()
            );

            // when
            Member local = beforeRegistration.localMember();

            // then
            assertThat(local.local()).isTrue();
            assertThat(local.endpoint()).isNull();
            assertThat(local.name()).isEqualTo("UNIVERSITY[LOCAL]");
        }

        @Test
        void namesThisApplicationByItsUriOnceItHasRegistered() {
            // when
            testSubject.onInstanceRegistered(new InstanceRegisteredEvent<>(this, localInstance));

            // then
            Member local = testSubject.localMember();
            assertThat(local.name()).isEqualTo("UNIVERSITY[http://node-a:8080]");
            assertThat(local.endpoint()).hasToString("http://node-a:8080");
        }

        @Test
        void survivesADiscoveryRoundWhileAnInstanceHasNoUriYet() {
            // given
            TestServiceInstance unregistered = TestServiceInstance.withoutUri("university");
            RecordingCapabilityDiscoveryMode mode = new RecordingCapabilityDiscoveryMode()
                    .answering(remoteInstance, HANDLES_CREATE);
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient().register("university", remoteInstance), unregistered, mode
            );

            // when
            registry.updateMemberships();

            // then — the round completes, with the reachable member on the ring
            assertThat(registry.ring().getMember("course-1", CREATE_COURSE)).isPresent();
        }

        @Test
        void appendsTheContextRootFromInstanceMetadata() {
            // given
            TestServiceInstance behindContextRoot = TestServiceInstance.instance("university", "node-c", 8080)
                                                                      .withMetadata("contextRoot", "/university");
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient().register("university", behindContextRoot),
                    localInstance,
                    new RecordingCapabilityDiscoveryMode().answering(behindContextRoot, HANDLES_CREATE),
                    instance -> true,
                    "contextRoot"
            );

            // when
            registry.updateMemberships();

            // then
            assertThat(registry.ring().getMembers())
                    .extracting(Member::endpoint)
                    .extracting(Object::toString)
                    .containsExactly("http://node-c:8080/university");
        }

        @Test
        void servesAnInstanceFromTheRootWhenItHasNoContextRootMetadata() {
            // given
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient().register("university", remoteInstance),
                    localInstance,
                    new RecordingCapabilityDiscoveryMode().answering(remoteInstance, HANDLES_CREATE),
                    instance -> true,
                    "contextRoot"
            );

            // when
            registry.updateMemberships();

            // then
            assertThat(registry.ring().getMembers())
                    .extracting(Member::endpoint)
                    .extracting(Object::toString)
                    .containsExactly("http://node-b:8080");
        }
    }

    @Nested
    class Suspecting {

        @Test
        void takesAnUnreachableMemberOutOfTheRing() {
            // given
            testSubject.updateMemberships();
            Member unreachable = testSubject.ring().getMembers().stream()
                                            .filter(member -> !member.local())
                                            .findFirst()
                                            .orElseThrow();

            // when
            testSubject.suspect(unreachable);

            // then
            assertThat(testSubject.ring().getMembers()).doesNotContain(unreachable);
        }

        @Test
        void bringsTheMemberBackOnTheNextDiscoveryRound() {
            // given
            testSubject.updateMemberships();
            Member unreachable = testSubject.ring().getMembers().stream()
                                            .filter(member -> !member.local())
                                            .findFirst()
                                            .orElseThrow();
            testSubject.suspect(unreachable);

            // when
            testSubject.updateMemberships();

            // then — suspecting only keeps commands away while a member is actually unreachable
            assertThat(testSubject.ring().getMembers()).contains(unreachable);
        }

        @Test
        void ignoresAMemberThatIsNotOnTheRing() {
            // given
            testSubject.updateMemberships();
            int versionBefore = testSubject.ring().version();

            // when
            testSubject.suspect(new Member("UNKNOWN[http://node-z:8080]",
                                          URI.create("http://node-z:8080"), false));

            // then
            assertThat(testSubject.ring().version()).isEqualTo(versionBefore);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsNullCollaborators() {
            // when / then
            assertThatThrownBy(() -> new SpringCloudMemberRegistry(null, localInstance, discoveryMode))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new SpringCloudMemberRegistry(discoveryClient, null, discoveryMode))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new SpringCloudMemberRegistry(discoveryClient, localInstance, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
