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
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the wire shape of {@link MemberCapabilitiesPayload} and the entity tag the capabilities endpoint reports.
 *
 * @author Allard Buijze
 */
class MemberCapabilitiesPayloadTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final String NODE_ID = "node-a-process";

    @Nested
    class Conversion {

        @Test
        void roundTripsCapabilities() {
            // given
            MemberCapabilities capabilities =
                    new MemberCapabilities(75, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of());

            // when
            MemberCapabilities roundTripped = MemberCapabilitiesPayload.from(NODE_ID, capabilities).toCapabilities();

            // then
            assertThat(roundTripped).isEqualTo(capabilities);
        }

        @Test
        void carriesTheNodeIdOfTheMember() {
            // when
            MemberCapabilitiesPayload payload = MemberCapabilitiesPayload.from(NODE_ID, MemberCapabilities.INCAPABLE);

            // then
            assertThat(payload.nodeId()).isEqualTo(NODE_ID);
        }

        @Test
        void readsAPayloadWithoutANodeId() {
            // given / when — an answer that does not identify the member, which a reader leaves out of the ring
            MemberCapabilitiesPayload payload = new MemberCapabilitiesPayload(null, 50, List.of(), List.of());

            // then
            assertThat(payload.nodeId()).isNull();
        }

        @Test
        void carriesNamesAsPlainStrings() {
            // given
            MemberCapabilities capabilities = new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of());

            // when
            MemberCapabilitiesPayload payload = MemberCapabilitiesPayload.from(NODE_ID, capabilities);

            // then — the JSON on the wire must not depend on QualifiedName's record shape
            assertThat(payload.commands()).containsExactly("university.CreateCourse");
        }

        @Test
        void servesAnEmptyQuerySet() {
            // given
            MemberCapabilities capabilities = new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of());

            // when
            MemberCapabilitiesPayload payload = MemberCapabilitiesPayload.from(NODE_ID, capabilities);

            // then — the field is present from the first release so the endpoint shape does not change later
            assertThat(payload.queries()).isEmpty();
        }

        @Test
        void readsAPayloadThatOmitsItsNameLists() {
            // given / when — a member that leaves the fields out altogether
            MemberCapabilitiesPayload payload = new MemberCapabilitiesPayload(NODE_ID, 50, null, null);

            // then
            assertThat(payload.commands()).isEmpty();
            assertThat(payload.queries()).isEmpty();
            assertThat(payload.toCapabilities()).isEqualTo(new MemberCapabilities(50, Set.of(), Set.of()));
        }
    }

    @Nested
    class EntityTag {

        @Test
        void isStableForUnchangedCapabilities() {
            // given
            MemberCapabilities capabilities = new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of());

            // when
            String first = MemberCapabilitiesPayload.from(NODE_ID, capabilities).entityTag();
            String second = MemberCapabilitiesPayload.from(NODE_ID, capabilities).entityTag();

            // then — an unstable tag would make every poll look like a change, defeating the point of the ETag
            assertThat(first).isEqualTo(second);
        }

        @Test
        void isIndependentOfTheOrderNamesWereAddedIn() {
            // given
            MemberCapabilitiesPayload oneOrder =
                    new MemberCapabilitiesPayload(NODE_ID,
                                                  75,
                                                  List.of("university.CreateCourse", "university.RenameCourse"),
                                                  List.of());
            MemberCapabilitiesPayload otherOrder = MemberCapabilitiesPayload.from(NODE_ID, 
                    new MemberCapabilities(75, Set.of(RENAME_COURSE, CREATE_COURSE), Set.of())
            );

            // when / then
            assertThat(otherOrder.entityTag()).isEqualTo(oneOrder.entityTag());
        }

        @Test
        void changesWhenACommandIsAdded() {
            // given
            String before = MemberCapabilitiesPayload
                    .from(NODE_ID, new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of())).entityTag();

            // when
            String after = MemberCapabilitiesPayload
                    .from(NODE_ID, new MemberCapabilities(75, Set.of(CREATE_COURSE, RENAME_COURSE), Set.of()))
                    .entityTag();

            // then
            assertThat(after).isNotEqualTo(before);
        }

        @Test
        void changesWhenTheLoadFactorChanges() {
            // given
            String before = MemberCapabilitiesPayload
                    .from(NODE_ID, new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of())).entityTag();

            // when
            String after = MemberCapabilitiesPayload
                    .from(NODE_ID, new MemberCapabilities(150, Set.of(CREATE_COURSE), Set.of())).entityTag();

            // then
            assertThat(after).isNotEqualTo(before);
        }

        @Test
        void changesWithTheNodeId() {
            // given — an application restarted at the same address, with unchanged capabilities
            MemberCapabilities capabilities = new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of());
            String before = MemberCapabilitiesPayload.from(NODE_ID, capabilities).entityTag();

            // when
            String after = MemberCapabilitiesPayload.from("node-a-restarted", capabilities).entityTag();

            // then — otherwise a member holding the previous tag would be told nothing changed, and keep the old id
            assertThat(after).isNotEqualTo(before);
        }

        @Test
        void isQuotedForUseAsAHeaderValue() {
            // given
            MemberCapabilitiesPayload payload = MemberCapabilitiesPayload.from(
                    NODE_ID, new MemberCapabilities(75, Set.of(CREATE_COURSE), Set.of())
            );

            // when / then — an ETag header value has to be quoted
            assertThat(payload.quotedEntityTag()).isEqualTo("\"" + payload.entityTag() + "\"");
        }
    }
}
