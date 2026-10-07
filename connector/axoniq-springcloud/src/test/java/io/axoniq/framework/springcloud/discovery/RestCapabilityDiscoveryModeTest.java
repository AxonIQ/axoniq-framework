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

package io.axoniq.framework.springcloud.discovery;

import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import io.axoniq.framework.springcloud.util.StubClientHttpRequestFactory;
import io.axoniq.framework.springcloud.util.TestServiceInstance;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link RestCapabilityDiscoveryMode} discovers members over HTTP, and how it treats the answers it gets.
 *
 * @author Steven van Beelen
 * @author Allard Buijze
 */
class RestCapabilityDiscoveryModeTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final String NODE_B = "node-b-process";
    private static final String CAPABILITIES_JSON =
            "{\"nodeId\":\"" + NODE_B + "\",\"loadFactor\":100,\"commands\":[\"university.CreateCourse\"],"
                    + "\"queries\":[]}";
    private static final MemberAdvertisement ADVERTISEMENT =
            new MemberAdvertisement(NODE_B, new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of()));
    private static final String E_TAG = "\"abc123\"";

    private TestServiceInstance remoteInstance;
    private StubClientHttpRequestFactory requestFactory;
    private RestCapabilityDiscoveryMode testSubject;

    @BeforeEach
    void setUp() {
        remoteInstance = TestServiceInstance.instance("university", "node-b", 8080);
        requestFactory = new StubClientHttpRequestFactory();
        RestClient restClient = RestClient.builder().requestFactory(requestFactory).build();
        testSubject = new RestCapabilityDiscoveryMode(restClient);
    }

    @Nested
    class DiscoveringAMember {

        @Test
        void readsTheNodeIdAndCapabilitiesItServes() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);

            // when
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then
            assertThat(advertisement).contains(ADVERTISEMENT);
        }

        @Test
        void asksTheConfiguredEndpointOnTheMembersOwnUri() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);

            // when
            testSubject.discover(remoteInstance);

            // then
            assertThat(requestFactory.lastRequest().getURI())
                    .hasToString("http://node-b:8080" + RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_ENDPOINT);
        }

        @Test
        void asksAConfiguredCustomEndpoint() {
            // given
            RestClient restClient = RestClient.builder().requestFactory(requestFactory).build();
            RestCapabilityDiscoveryMode customEndpoint =
                    new RestCapabilityDiscoveryMode(restClient, "/custom/capabilities");
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);

            // when
            customEndpoint.discover(remoteInstance);

            // then
            assertThat(requestFactory.lastRequest().getURI()).hasToString("http://node-b:8080/custom/capabilities");
        }
    }

    @Nested
    class IdentifyingThisApplication {

        @Test
        void keepsOneNodeIdForItsWholeLifetime() {
            // when / then
            assertThat(testSubject.localNodeId()).isNotBlank().isEqualTo(testSubject.localNodeId());
        }

        @Test
        void givesEveryApplicationANodeIdOfItsOwn() {
            // given — two applications, or one application restarted
            RestCapabilityDiscoveryMode other =
                    new RestCapabilityDiscoveryMode(RestClient.builder().requestFactory(requestFactory).build());

            // when / then — a restarted application is a new member, which has not seen what its predecessor did
            assertThat(other.localNodeId()).isNotEqualTo(testSubject.localNodeId());
        }

        @Test
        void servesWhatWasPublishedLocally() {
            // given
            MemberCapabilities published = new MemberCapabilities(100, Set.of(RENAME_COURSE), Set.of());

            // when
            testSubject.updateLocalCapabilities(published);

            // then
            assertThat(testSubject.localCapabilities()).isEqualTo(published);
        }

        @Test
        void reportsNothingPublishedYetAsHandlingNothing() {
            // when / then
            assertThat(testSubject.localCapabilities()).isEqualTo(MemberCapabilities.INCAPABLE);
        }
    }

    @Nested
    class Revalidating {

        @Test
        void sendsNoConditionalHeaderOnTheFirstRequest() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);

            // when
            testSubject.discover(remoteInstance);

            // then
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }

        @Test
        void sendsTheTagTheMemberItselfIssuedOnTheNextRequest() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).respondingNotModified();
            testSubject.discover(remoteInstance);

            // when
            testSubject.discover(remoteInstance);

            // then — echoing the member's own tag is what makes the conditional request meaningful; a tag derived
            // locally would only match by coincidence
            assertThat(requestFactory.lastRequest().getHeaders().getIfNoneMatch()).containsExactly(E_TAG);
        }

        @Test
        void sendsNoConditionalHeaderToAMemberThatIssuesNoTag() {
            // given — a member, or a proxy in front of it, that does not set an ETag
            requestFactory.respondingWith(CAPABILITIES_JSON, null).respondingWith(CAPABILITIES_JSON, null);
            testSubject.discover(remoteInstance);

            // when
            testSubject.discover(remoteInstance);

            // then — there is nothing to revalidate against, so the full payload is read each time
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }

        @Test
        void reusesTheCachedAdvertisementWhenTheMemberIsUnchanged() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).respondingNotModified();
            testSubject.discover(remoteInstance);

            // when — the member answers 304 with no body
            Optional<MemberAdvertisement> second = testSubject.discover(remoteInstance);

            // then — this is the point of the ETag: a steady-state poll costs a round trip and nothing more
            assertThat(second).contains(ADVERTISEMENT);
        }

        @Test
        void readsTheNewCapabilitiesWhenTheMemberHasChanged() {
            // given
            requestFactory
                    .respondingWith(CAPABILITIES_JSON, E_TAG)
                    .respondingWith("{\"nodeId\":\"" + NODE_B + "\",\"loadFactor\":100,\"commands\":"
                                            + "[\"university.CreateCourse\",\"university.RenameCourse\"],"
                                            + "\"queries\":[]}", "\"def456\"");
            testSubject.discover(remoteInstance);

            // when
            Optional<MemberAdvertisement> updated = testSubject.discover(remoteInstance);

            // then
            assertThat(updated).map(MemberAdvertisement::capabilities).contains(
                    new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of())
            );
        }

        @Test
        void readsTheNewNodeIdOfAMemberRestartedAtTheSameAddress() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG)
                          .respondingWith(CAPABILITIES_JSON.replace(NODE_B, "node-b-restarted"), "\"ghi789\"");
            testSubject.discover(remoteInstance);

            // when
            Optional<MemberAdvertisement> afterRestart = testSubject.discover(remoteInstance);

            // then
            assertThat(afterRestart).map(MemberAdvertisement::nodeId).contains("node-b-restarted");
        }

        @Test
        void forgetsWhatItCachedForAMemberThatIsGone() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.discover(remoteInstance);

            // when
            testSubject.retainOnly(Set.of());
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.discover(remoteInstance);

            // then — no stale tag is sent for an instance that went away and came back
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }
    }

    @Nested
    class WhenTheMemberAnswersBadly {

        @Test
        void reportsAClientErrorAsAnInstanceNotServingCapabilities() {
            // given — another service altogether, or an older version of this one
            requestFactory.respondingWithStatus(HttpStatus.NOT_FOUND);

            // when / then
            assertThatThrownBy(() -> testSubject.discover(remoteInstance))
                    .isInstanceOf(ServiceInstanceClientException.class)
                    .hasMessageContaining("404");
        }

        @Test
        void keepsAMemberThatFailsToAnswerAsTheSameMemberHandlingNothing() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG)
                          .respondingWithStatus(HttpStatus.SERVICE_UNAVAILABLE);
            testSubject.discover(remoteInstance);

            // when
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then — the member is expected back, so it stays in the ring handling nothing rather than being removed
            assertThat(advertisement).contains(new MemberAdvertisement(NODE_B, MemberCapabilities.INCAPABLE));
        }

        @Test
        void keepsAMemberThatCannotBeConnectedToAsTheSameMemberHandlingNothing() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).failingToConnect();
            testSubject.discover(remoteInstance);

            // when
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then
            assertThat(advertisement).contains(new MemberAdvertisement(NODE_B, MemberCapabilities.INCAPABLE));
        }

        @Test
        void leavesOutAnInstanceThatNeverAnswered() {
            // given
            requestFactory.respondingWithStatus(HttpStatus.INTERNAL_SERVER_ERROR);

            // when
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then — without an answer there is no node id to tell the instance apart by
            assertThat(advertisement).isEmpty();
        }

        @Test
        void leavesOutAnInstanceAnsweringWithoutANodeId() {
            // given
            requestFactory.respondingWith("{\"loadFactor\":100,\"commands\":[\"university.CreateCourse\"]}", E_TAG);

            // when
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then
            assertThat(advertisement).isEmpty();
        }

        @Test
        void forgetsTheNodeIdOfAMemberThatIsGone() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.discover(remoteInstance);
            testSubject.retainOnly(Set.of());

            // when
            requestFactory.failingToConnect();
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then
            assertThat(advertisement).isEmpty();
        }

        @Test
        void discardsTheCachedTagWhenAMemberStopsAnswering() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).failingToConnect();
            testSubject.discover(remoteInstance);
            testSubject.discover(remoteInstance);

            // when
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.discover(remoteInstance);

            // then — revalidating against a tag whose cached value was dropped would yield a 304 with nothing to
            // fall back on
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }

        @Test
        void reportsAnEmptyBodyFromAKnownMemberAsTheSameMemberHandlingNothing() {
            // given — a 200 with no body, which the member should never send
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).respondingWithStatus(HttpStatus.OK);
            testSubject.discover(remoteInstance);

            // when
            Optional<MemberAdvertisement> advertisement = testSubject.discover(remoteInstance);

            // then
            assertThat(advertisement).contains(new MemberAdvertisement(NODE_B, MemberCapabilities.INCAPABLE));
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsNullArguments() {
            // when / then
            assertThatThrownBy(() -> new RestCapabilityDiscoveryMode(null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.discover(null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.updateLocalCapabilities(null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.retainOnly(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
