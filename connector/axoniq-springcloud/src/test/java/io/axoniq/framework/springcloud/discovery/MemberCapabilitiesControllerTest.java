/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.discovery;

import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the conditional-request contract of the capabilities endpoint, which is what keeps a steady-state cluster from
 * re-reading every member's capabilities on every discovery heartbeat.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 */
class MemberCapabilitiesControllerTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");

    private RecordingCapabilityDiscoveryMode discoveryMode;
    private MemberCapabilitiesController testSubject;

    @BeforeEach
    void setUp() {
        discoveryMode = new RecordingCapabilityDiscoveryMode();
        discoveryMode.updateLocalCapabilities(new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of()));
        testSubject = new MemberCapabilitiesController(discoveryMode);
    }

    @Nested
    class WhenTheMemberHasNotSeenTheseCapabilities {

        @Test
        void servesThemWithAnEntityTag() {
            // when
            ResponseEntity<MemberCapabilitiesPayload> response = testSubject.localMemberCapabilities(null);

            // then
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().nodeId()).isEqualTo(RecordingCapabilityDiscoveryMode.LOCAL_NODE_ID);
            assertThat(response.getBody().commands()).containsExactly(CREATE_COURSE.name());
            assertThat(response.getBody().loadFactor()).isEqualTo(100);
            assertThat(response.getHeaders().getETag()).isNotBlank();
        }

        @Test
        void servesThemWhenTheEntityTagNoLongerMatches() {
            // given — the tag of some earlier capability set
            String staleTag = "\"an-earlier-tag\"";

            // when
            ResponseEntity<MemberCapabilitiesPayload> response = testSubject.localMemberCapabilities(staleTag);

            // then
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
        }
    }

    @Nested
    class WhenTheMemberAlreadySawTheseCapabilities {

        @Test
        void answersNotModifiedWithNoBody() {
            // given — the tag from a first request, as a polling member would hold
            String entityTag = testSubject.localMemberCapabilities(null).getHeaders().getETag();

            // when
            ResponseEntity<MemberCapabilitiesPayload> response = testSubject.localMemberCapabilities(entityTag);

            // then — this is what a steady-state heartbeat costs: a round trip and nothing else
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_MODIFIED);
            assertThat(response.getBody()).isNull();
            assertThat(response.getHeaders().getETag()).isEqualTo(entityTag);
        }

        @Test
        void servesThemAgainOnceTheCapabilitiesChange() {
            // given
            String entityTag = testSubject.localMemberCapabilities(null).getHeaders().getETag();
            discoveryMode.updateLocalCapabilities(
                    new MemberCapabilities(100, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of())
            );

            // when — the member polls with the tag it last saw
            ResponseEntity<MemberCapabilitiesPayload> response = testSubject.localMemberCapabilities(entityTag);

            // then
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().commands())
                    .containsExactlyInAnyOrder(CREATE_COURSE.name(), RENAME_COURSE.name());
            assertThat(response.getHeaders().getETag()).isNotEqualTo(entityTag);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullDiscoveryMode() {
            // when / then
            assertThatThrownBy(() -> new MemberCapabilitiesController(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
