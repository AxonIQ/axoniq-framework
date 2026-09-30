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
 * Tests how {@link RestCapabilityDiscoveryMode} discovers capabilities over HTTP, and how it treats the answers it
 * gets.
 *
 * @author Steven van Beelen
 * @author Allard Buijze
 */
class RestCapabilityDiscoveryModeTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final String CAPABILITIES_JSON =
            "{\"loadFactor\":100,\"commands\":[\"university.CreateCourse\"],\"queries\":[]}";
    private static final String E_TAG = "\"abc123\"";

    private TestServiceInstance localInstance;
    private TestServiceInstance remoteInstance;
    private StubClientHttpRequestFactory requestFactory;
    private RestCapabilityDiscoveryMode testSubject;

    @BeforeEach
    void setUp() {
        localInstance = TestServiceInstance.instance("university", "node-a", 8080);
        remoteInstance = TestServiceInstance.instance("university", "node-b", 8080);
        requestFactory = new StubClientHttpRequestFactory();
        RestClient restClient = RestClient.builder().requestFactory(requestFactory).build();
        testSubject = new RestCapabilityDiscoveryMode(restClient);
        testSubject.updateLocalCapabilities(localInstance, MemberCapabilities.INCAPABLE);
    }

    @Nested
    class DiscoveringAnotherMember {

        @Test
        void readsTheCapabilitiesItServes() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);

            // when
            Optional<MemberCapabilities> capabilities = testSubject.capabilities(remoteInstance);

            // then
            assertThat(capabilities).contains(new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of()));
        }

        @Test
        void asksTheConfiguredEndpointOnTheMembersOwnUri() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);

            // when
            testSubject.capabilities(remoteInstance);

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
            customEndpoint.capabilities(remoteInstance);

            // then
            assertThat(requestFactory.lastRequest().getURI()).hasToString("http://node-b:8080/custom/capabilities");
        }
    }

    @Nested
    class DiscoveringThisMember {

        @Test
        void answersFromWhatWasPublishedLocallyWithoutMakingARequest() {
            // given
            MemberCapabilities published = new MemberCapabilities(100, Set.of(RENAME_COURSE), Set.of());
            testSubject.updateLocalCapabilities(localInstance, published);

            // when
            Optional<MemberCapabilities> capabilities = testSubject.capabilities(localInstance);

            // then — asking this application for its own capabilities over HTTP would be pointless, and would fail
            // while it is still starting up
            assertThat(capabilities).contains(published);
            assertThat(requestFactory.requests()).isEmpty();
        }

        @Test
        void recognisesItsOwnInstanceEvenThroughADifferentObject() {
            // given — discovery reports its own object for this application, not the Registration it was given
            testSubject.updateLocalCapabilities(localInstance, MemberCapabilities.INCAPABLE);
            TestServiceInstance sameInstanceReportedByDiscovery =
                    TestServiceInstance.instance("university", "node-a", 8080);

            // when
            testSubject.capabilities(sameInstanceReportedByDiscovery);

            // then
            assertThat(requestFactory.requests()).isEmpty();
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
            testSubject.capabilities(remoteInstance);

            // then
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }

        @Test
        void sendsTheTagTheMemberItselfIssuedOnTheNextRequest() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).respondingNotModified();
            testSubject.capabilities(remoteInstance);

            // when
            testSubject.capabilities(remoteInstance);

            // then — echoing the member's own tag is what makes the conditional request meaningful; a tag derived
            // locally would only match by coincidence
            assertThat(requestFactory.lastRequest().getHeaders().getIfNoneMatch()).containsExactly(E_TAG);
        }

        @Test
        void sendsNoConditionalHeaderToAMemberThatIssuesNoTag() {
            // given — a member, or a proxy in front of it, that does not set an ETag
            requestFactory.respondingWith(CAPABILITIES_JSON, null).respondingWith(CAPABILITIES_JSON, null);
            testSubject.capabilities(remoteInstance);

            // when
            testSubject.capabilities(remoteInstance);

            // then — there is nothing to revalidate against, so the full payload is read each time
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }

        @Test
        void reusesTheCachedCapabilitiesWhenTheMemberIsUnchanged() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).respondingNotModified();
            MemberCapabilities first = testSubject.capabilities(remoteInstance).orElseThrow();

            // when — the member answers 304 with no body
            Optional<MemberCapabilities> second = testSubject.capabilities(remoteInstance);

            // then — this is the point of the ETag: a steady-state poll costs a round trip and nothing more
            assertThat(second).contains(first);
        }

        @Test
        void readsTheNewCapabilitiesWhenTheMemberHasChanged() {
            // given
            requestFactory
                    .respondingWith(CAPABILITIES_JSON, E_TAG)
                    .respondingWith("{\"loadFactor\":100,\"commands\":[\"university.CreateCourse\","
                                            + "\"university.RenameCourse\"],\"queries\":[]}", "\"def456\"");
            testSubject.capabilities(remoteInstance);

            // when
            Optional<MemberCapabilities> updated = testSubject.capabilities(remoteInstance);

            // then
            assertThat(updated).contains(
                    new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of())
            );
        }

        @Test
        void forgetsWhatItCachedForAMemberThatIsGone() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.capabilities(remoteInstance);

            // when
            testSubject.retainOnly(Set.of(ServiceInstanceKey.of(localInstance)));
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.capabilities(remoteInstance);

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
            assertThatThrownBy(() -> testSubject.capabilities(remoteInstance))
                    .isInstanceOf(ServiceInstanceClientException.class)
                    .hasMessageContaining("404");
        }

        @Test
        void reportsAServerErrorAsAMemberHandlingNothing() {
            // given
            requestFactory.respondingWithStatus(HttpStatus.INTERNAL_SERVER_ERROR);

            // when
            Optional<MemberCapabilities> capabilities = testSubject.capabilities(remoteInstance);

            // then — the member is expected back, so it stays in the ring handling nothing rather than being removed
            assertThat(capabilities).contains(MemberCapabilities.INCAPABLE);
        }

        @Test
        void reportsAConnectionFailureAsAMemberHandlingNothing() {
            // given
            requestFactory.failingToConnect();

            // when
            Optional<MemberCapabilities> capabilities = testSubject.capabilities(remoteInstance);

            // then
            assertThat(capabilities).contains(MemberCapabilities.INCAPABLE);
        }

        @Test
        void discardsTheCachedTagWhenAMemberStopsAnswering() {
            // given
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG).failingToConnect();
            testSubject.capabilities(remoteInstance);
            testSubject.capabilities(remoteInstance);

            // when
            requestFactory.respondingWith(CAPABILITIES_JSON, E_TAG);
            testSubject.capabilities(remoteInstance);

            // then — revalidating against a tag whose cached value was dropped would yield a 304 with nothing to
            // fall back on
            assertThat(requestFactory.lastRequest().getHeaders().get(HttpHeaders.IF_NONE_MATCH)).isNull();
        }

        @Test
        void reportsAnEmptyBodyAsAMemberHandlingNothing() {
            // given — a 200 with no body, which the member should never send
            requestFactory.respondingWithStatus(HttpStatus.OK);

            // when
            Optional<MemberCapabilities> capabilities = testSubject.capabilities(remoteInstance);

            // then
            assertThat(capabilities).contains(MemberCapabilities.INCAPABLE);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsNullArguments() {
            // when / then
            assertThatThrownBy(() -> new RestCapabilityDiscoveryMode(null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.capabilities(null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.retainOnly(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
