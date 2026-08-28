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

import org.junit.jupiter.api.*;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the construction rules of a {@link Member}.
 *
 * @author Allard Buijze
 */
class MemberTest {

    @Test
    void acceptsALocalMemberWithoutAnEndpoint() {
        // given / when — an application that has not finished registering does not know its own URI yet
        Member member = Member.unregisteredLocalMember("SERVICE[LOCAL]");

        // then
        assertThat(member.local()).isTrue();
        assertThat(member.endpoint()).isNull();
    }

    @Test
    void rejectsARemoteMemberWithoutAnEndpoint() {
        // when / then — a remote member without an endpoint could never be reached, so it must not exist
        assertThatThrownBy(() -> new Member("SERVICE[remote]", null, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires an endpoint");
    }

    @Test
    void rejectsAMemberWithoutAName() {
        // when / then
        assertThatThrownBy(() -> new Member(null, URI.create("http://node-a:8080"), false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void distinguishesMembersByNameAndEndpoint() {
        // given
        Member first = new Member("SERVICE[http://node-a:8080]", URI.create("http://node-a:8080"), false);
        Member same = new Member("SERVICE[http://node-a:8080]", URI.create("http://node-a:8080"), false);
        Member other = new Member("SERVICE[http://node-b:8080]", URI.create("http://node-b:8080"), false);

        // when / then
        assertThat(first).isEqualTo(same).hasSameHashCodeAs(same).isNotEqualTo(other);
    }
}
