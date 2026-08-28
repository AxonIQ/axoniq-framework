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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the construction rules and command membership test of {@link MemberCapabilities}.
 *
 * @author Allard Buijze
 */
class MemberCapabilitiesTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");

    @Test
    void reportsWhetherACommandIsHandled() {
        // given
        MemberCapabilities capabilities = new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of());

        // when / then
        assertThat(capabilities.handlesCommand(CREATE_COURSE)).isTrue();
        assertThat(capabilities.handlesCommand(RENAME_COURSE)).isFalse();
    }

    @Test
    void handlesNothingWhenIncapable() {
        // when / then
        assertThat(MemberCapabilities.INCAPABLE.handlesCommand(CREATE_COURSE)).isFalse();
        assertThat(MemberCapabilities.INCAPABLE.loadFactor()).isZero();
    }

    @Test
    void copiesTheGivenNameSets() {
        // given
        Set<QualifiedName> commands = new HashSet<>(Set.of(CREATE_COURSE));

        // when
        MemberCapabilities capabilities = new MemberCapabilities(100, commands, Set.of());
        commands.add(RENAME_COURSE);

        // then — a caller mutating its set afterwards must not change what this member advertises
        assertThat(capabilities.handlesCommand(RENAME_COURSE)).isFalse();
    }

    @Test
    void rejectsANegativeLoadFactor() {
        // when / then
        assertThatThrownBy(() -> new MemberCapabilities(-1, Set.of(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("load factor");
    }

    @Test
    void rejectsNullNameSets() {
        // when / then
        assertThatThrownBy(() -> new MemberCapabilities(100, null, Set.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MemberCapabilities(100, Set.of(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
