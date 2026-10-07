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

import io.axoniq.framework.springcloud.discovery.RecordingCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.ServiceInstanceKey;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import io.axoniq.framework.springcloud.util.RecordingDiscoveryClient;
import io.axoniq.framework.springcloud.util.TestServiceInstance;
import org.axonframework.messaging.core.QualifiedName;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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
                .answeringAsLocal(localInstance)
                .answering(remoteInstance, HANDLES_CREATE);
        testSubject = registry(SpringCloudMemberRegistry.DEFAULT_CAPABILITIES_REFRESH_INTERVAL);
    }

    /**
     * Builds a registry over this test's discovery that runs its rounds on the calling thread, so a test observes a
     * round's outcome as soon as it starts one.
     */
    private SpringCloudMemberRegistry registry(Duration capabilitiesRefreshInterval) {
        return new SpringCloudMemberRegistry(discoveryClient,
                                             discoveryMode,
                                             instance -> true,
                                             null,
                                             capabilitiesRefreshInterval,
                                             Runnable::run);
    }

    @Nested
    class BuildingTheRing {

        @Test
        void includesEveryDiscoveredInstanceThatReportsCapabilities() {
            // when
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().members()).hasSize(2);
        }

        @Test
        void marksThisApplicationsOwnInstanceAsLocal() {
            // given
            testSubject.onInstanceRegistered(new InstanceRegisteredEvent<>(this, localInstance));

            // when
            Set<Member> members = testSubject.ring().members();

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
            assertThat(testSubject.ring().members()).hasSize(1);
        }

        @Test
        void leavesOutInstancesWhoseCapabilitiesCouldNotBeDiscovered() {
            // given
            discoveryMode.failingWithClientError(remoteInstance);

            // when
            testSubject.updateMemberships();

            // then — the round must not be abandoned because one instance threw
            assertThat(testSubject.ring().members()).hasSize(1);
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
            assertThat(testSubject.ring().members()).hasSize(2);

            // when — a member leaves
            discoveryClient.deregister("university", remoteInstance);
            testSubject.onHeartbeat(new HeartbeatEvent(this, "second"));

            // then — rebuilding means a departed member simply does not reappear
            assertThat(testSubject.ring().members()).hasSize(1);
        }

        @Test
        void asksOnlyInstancesItHasNoCurrentCapabilitiesForOnAMembershipRound() {
            // given
            testSubject.updateMemberships();
            TestServiceInstance joining = TestServiceInstance.instance("university", "node-c", 8080);
            discoveryClient.register("university", joining);
            discoveryMode.answering(joining, HANDLES_CREATE);
            int askedBefore = discoveryMode.asked().size();

            // when
            testSubject.updateMemberships();

            // then — membership is what discovery signals, so only the new instance needed asking
            assertThat(discoveryMode.asked().subList(askedBefore, discoveryMode.asked().size()))
                    .containsExactly(ServiceInstanceKey.of(joining));
            assertThat(testSubject.ring().members()).hasSize(3);
        }

        @Test
        void asksEveryInstanceOnEveryMembershipRoundWithoutARefreshInterval() {
            // given
            SpringCloudMemberRegistry alwaysAsking = registry(Duration.ZERO);
            alwaysAsking.updateMemberships();

            // when
            discoveryMode.answering(remoteInstance,
                                    new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of()));
            alwaysAsking.updateMemberships();

            // then
            assertThat(alwaysAsking.ring().memberFor("course-1", RENAME_COURSE)).isPresent();
        }

        @Test
        void startsARoundOnAHeartbeatOnItsExecutorRatherThanOnTheHeartbeatThread() {
            // given
            List<Runnable> handedOff = new ArrayList<>();
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    discoveryClient, discoveryMode, instance -> true, null, Duration.ofSeconds(30), handedOff::add
            );

            // when
            registry.onHeartbeat(new HeartbeatEvent(this, "first"));

            // then — the heartbeat thread does not wait on the requests the round makes
            assertThat(handedOff).hasSize(1);
            assertThat(registry.ring().members()).isEmpty();
        }
    }

    @Nested
    class RefreshingCapabilities {

        @Test
        void leavesInstancesWithCurrentCapabilitiesAlone() {
            // given
            testSubject.updateMemberships();
            int askedBefore = discoveryMode.asked().size();

            // when
            testSubject.refreshCapabilities();

            // then
            assertThat(discoveryMode.asked()).hasSize(askedBefore);
        }

        @Test
        void picksUpCapabilitiesAMemberGainedOnceTheyAreStale() {
            // given
            SpringCloudMemberRegistry refreshing = registry(Duration.ZERO);
            refreshing.updateMemberships();
            discoveryMode.answering(remoteInstance,
                                    new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of()));

            // when — without discovery signalling anything, as with Spring Cloud Kubernetes
            refreshing.refreshCapabilities();

            // then
            assertThat(refreshing.ring().memberFor("course-1", RENAME_COURSE)).isPresent();
        }

        @Test
        void leavesWhoIsInTheClusterToDiscovery() {
            // given
            SpringCloudMemberRegistry refreshing = registry(Duration.ZERO);
            refreshing.updateMemberships();
            TestServiceInstance joining = TestServiceInstance.instance("university", "node-c", 8080);
            discoveryClient.register("university", joining);
            discoveryMode.answering(joining, HANDLES_CREATE);

            // when
            refreshing.refreshCapabilities();

            // then — the new instance waits for the membership round discovery's next signal starts
            assertThat(refreshing.ring().members()).hasSize(2);
        }

        @Test
        void asksAnInstanceThatHasNotAnsweredYetAgain() {
            // given — an instance that is still starting up
            discoveryMode.reportingUnknown(remoteInstance);
            testSubject.updateMemberships();
            assertThat(testSubject.ring().members()).hasSize(1);

            // when
            discoveryMode.answering(remoteInstance, HANDLES_CREATE);
            testSubject.refreshCapabilities();

            // then — it joins without waiting for its capabilities to go stale, as it has none
            assertThat(testSubject.ring().members()).hasSize(2);
        }

        @Test
        void refreshesOnTheGivenSchedule() {
            // given
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                SpringCloudMemberRegistry refreshing = registry(Duration.ofMillis(100));
                refreshing.updateMemberships();
                refreshing.scheduleCapabilityRefresh(scheduler);

                // when
                discoveryMode.answering(remoteInstance,
                                        new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of()));

                // then
                Awaitility.await()
                          .atMost(Duration.ofSeconds(5))
                          .until(() -> refreshing.ring().memberFor("course-1", RENAME_COURSE).isPresent());
            } finally {
                scheduler.shutdownNow();
            }
        }

        @Test
        void schedulesNothingWithoutARefreshInterval() {
            // given
            ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
            try {
                // when
                registry(Duration.ZERO).scheduleCapabilityRefresh(scheduler);

                // then
                assertThat(scheduler.getQueue()).isEmpty();
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Nested
    class IdentifyingMembers {

        @Test
        void identifiesThisApplicationByTheNodeIdOfItsDiscoveryMode() {
            // when
            Member local = testSubject.localMember();

            // then
            assertThat(local.name()).isEqualTo(RecordingCapabilityDiscoveryMode.LOCAL_NODE_ID);
            assertThat(local.local()).isTrue();
            assertThat(local.endpoint()).isNull();
        }

        @Test
        void namesEveryOtherMemberByTheNodeIdItAnswersWith() {
            // given
            discoveryMode.answeringAs(remoteInstance, "node-b-process", HANDLES_CREATE);

            // when
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().members())
                    .extracting(Member::name)
                    .containsExactlyInAnyOrder(RecordingCapabilityDiscoveryMode.LOCAL_NODE_ID, "node-b-process");
        }

        @Test
        void recognizesThisApplicationWhicheverServiceDiscoveryReportsItUnder() {
            // given — discovery reports this application under a service id it does not know itself by, as Spring
            // Cloud Kubernetes does with the name of the Service selecting the pod
            TestServiceInstance underAnotherService = TestServiceInstance.instance("courses-svc", "node-a", 8080);
            discoveryClient.deregisterAll().register("courses-svc", underAnotherService, remoteInstance);
            discoveryMode.answeringAsLocal(underAnotherService);

            // when
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().members()).filteredOn(Member::local)
                                                    .containsExactly(testSubject.localMember());
        }

        @Test
        void countsAnApplicationDiscoveryReportsTwiceAsOneMember() {
            // given — one application selected by two services, so discovery reports it once for each
            TestServiceInstance asCourses = TestServiceInstance.instance("courses", "node-b", 8080);
            TestServiceInstance asCatalog = TestServiceInstance.instance("catalog", "node-b", 8081);
            discoveryClient.deregisterAll()
                           .register("courses", asCourses)
                           .register("catalog", asCatalog);
            discoveryMode.answeringAs(asCourses, "node-b-process", HANDLES_CREATE)
                         .answeringAs(asCatalog, "node-b-process", HANDLES_CREATE);

            // when
            testSubject.updateMemberships();

            // then — reached at the instance discovery reported first, so every member picks the same one
            assertThat(testSubject.ring().members())
                    .singleElement()
                    .satisfies(member -> assertThat(member.endpoint()).hasToString("http://node-b:8080"));
        }

        @Test
        void keepsThisApplicationOnTheRingOnceWhenItsOwnHandlersChange() {
            // given
            testSubject.updateMemberships();

            // when
            testSubject.publishLocalCommands(100, Set.of(CREATE_COURSE, RENAME_COURSE));

            // then — publishing updates the member discovery placed, rather than adding a second one
            assertThat(testSubject.ring().members()).hasSize(2);
            assertThat(testSubject.ring().memberFor("course-1", RENAME_COURSE)).contains(testSubject.localMember());
        }

        @Test
        void leavesThisApplicationOutWhileDiscoveryDoesNotReportIt() {
            // given — handlers subscribed, but discovery does not report this application yet
            testSubject.publishLocalCommands(100, Set.of(RENAME_COURSE));
            discoveryClient.deregister("university", localInstance);

            // when
            testSubject.updateMemberships();

            // then — the other members do not route to this application either, so neither does it
            assertThat(testSubject.ring().members()).doesNotContain(testSubject.localMember());
        }

        @Test
        void appliesThisApplicationsCurrentCapabilitiesRatherThanWhatItsInstanceAnswered() {
            // given
            testSubject.publishLocalCommands(100, Set.of(RENAME_COURSE));

            // when
            testSubject.updateMemberships();

            // then
            assertThat(testSubject.ring().capabilitiesOf(testSubject.localMember()))
                    .hasValueSatisfying(capabilities -> assertThat(capabilities.commands())
                            .containsExactly(RENAME_COURSE));
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
            testSubject.publishLocalQueries(Set.of(FIND_COURSE));
            discoveryMode.answering(remoteInstance, new MemberCapabilities(100, Set.of(), Set.of(FIND_COURSE)));
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
    class ReachingMembers {

        @Test
        void survivesADiscoveryRoundWhileAnInstanceHasNoUriYet() {
            // given
            TestServiceInstance unregistered = TestServiceInstance.withoutUri("university");
            RecordingCapabilityDiscoveryMode mode = new RecordingCapabilityDiscoveryMode()
                    .answering(unregistered, HANDLES_RENAME)
                    .answering(remoteInstance, HANDLES_CREATE);
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient().register("university", unregistered, remoteInstance), mode
            );

            // when
            registry.updateMemberships();

            // then — the round completes, with the reachable member on the ring and the other left out
            assertThat(registry.ring().memberFor("course-1", CREATE_COURSE)).isPresent();
            assertThat(registry.ring().memberFor("course-1", RENAME_COURSE)).isEmpty();
        }

        @Test
        void appendsTheContextRootFromInstanceMetadata() {
            // given
            TestServiceInstance behindContextRoot = TestServiceInstance.instance("university", "node-c", 8080)
                                                                      .withMetadata("contextRoot", "/university");
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient().register("university", behindContextRoot),
                    new RecordingCapabilityDiscoveryMode().answering(behindContextRoot, HANDLES_CREATE),
                    instance -> true,
                    "contextRoot"
            );

            // when
            registry.updateMemberships();

            // then
            assertThat(registry.ring().members())
                    .extracting(Member::endpoint)
                    .extracting(Object::toString)
                    .containsExactly("http://node-c:8080/university");
        }

        @Test
        void servesAnInstanceFromTheRootWhenItHasNoContextRootMetadata() {
            // given
            SpringCloudMemberRegistry registry = new SpringCloudMemberRegistry(
                    new RecordingDiscoveryClient().register("university", remoteInstance),
                    new RecordingCapabilityDiscoveryMode().answering(remoteInstance, HANDLES_CREATE),
                    instance -> true,
                    "contextRoot"
            );

            // when
            registry.updateMemberships();

            // then
            assertThat(registry.ring().members())
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
            Member unreachable = testSubject.ring().members().stream()
                                            .filter(member -> !member.local())
                                            .findFirst()
                                            .orElseThrow();

            // when
            testSubject.markUnreachable(unreachable);

            // then
            assertThat(testSubject.ring().members()).doesNotContain(unreachable);
        }

        @Test
        void bringsTheMemberBackOnTheNextDiscoveryRound() {
            // given
            testSubject.updateMemberships();
            Member unreachable = testSubject.ring().members().stream()
                                            .filter(member -> !member.local())
                                            .findFirst()
                                            .orElseThrow();
            testSubject.markUnreachable(unreachable);

            // when
            testSubject.updateMemberships();

            // then — suspecting only keeps commands away while a member is actually unreachable
            assertThat(testSubject.ring().members()).contains(unreachable);
        }

        @Test
        void keepsTheMemberOutUntilItAnswersAgain() {
            // given
            testSubject.updateMemberships();
            Member unreachable = testSubject.ring().members().stream()
                                            .filter(member -> !member.local())
                                            .findFirst()
                                            .orElseThrow();
            testSubject.markUnreachable(unreachable);

            // when — the next round rebuilds the ring, but the member does not answer
            discoveryMode.reportingUnknown(remoteInstance);
            testSubject.updateMemberships();

            // then — what it answered before does not bring it back
            assertThat(testSubject.ring().members()).doesNotContain(unreachable);
        }

        @Test
        void ignoresAMemberThatIsNotOnTheRing() {
            // given
            testSubject.updateMemberships();
            int versionBefore = testSubject.ring().version();

            // when
            testSubject.markUnreachable(new Member("unknown-node", URI.create("http://node-z:8080"), false));

            // then
            assertThat(testSubject.ring().version()).isEqualTo(versionBefore);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsNullCollaborators() {
            // when / then
            assertThatThrownBy(() -> new SpringCloudMemberRegistry(null, discoveryMode))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new SpringCloudMemberRegistry(discoveryClient, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
