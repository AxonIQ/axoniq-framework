/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.routing;

import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the construction rules of {@link MemberCapabilities}, and the membership tests routing relies on.
 *
 * @author Allard Buijze
 */
class MemberCapabilitiesTest {

    private static final QualifiedName CREATE_COURSE = new QualifiedName("university.CreateCourse");
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final QualifiedName FIND_COURSE = new QualifiedName("university.FindCourse");
    private static final QualifiedName LIST_COURSES = new QualifiedName("university.ListCourses");

    @Nested
    class ReportingWhatIsHandled {

        @Test
        void reportsWhetherAQueryIsHandled() {
            // given a member handling one query and not another
            MemberCapabilities capabilities = new MemberCapabilities(0, Set.of(), Set.of(FIND_COURSE));

            // when / then this is what every query dispatch routes on
            assertThat(capabilities.handlesQuery(FIND_COURSE)).isTrue();
            assertThat(capabilities.handlesQuery(LIST_COURSES)).isFalse();
        }

        @Test
        void keepsCommandAndQueryNamesApart() {
            // given the same name subscribed as a command only
            MemberCapabilities capabilities = new MemberCapabilities(100, Set.of(CREATE_COURSE), Set.of(FIND_COURSE));

            // when / then a command name must not make the member a candidate for a query of that name
            assertThat(capabilities.handlesCommand(CREATE_COURSE)).isTrue();
            assertThat(capabilities.handlesQuery(CREATE_COURSE)).isFalse();
            assertThat(capabilities.handlesQuery(FIND_COURSE)).isTrue();
            assertThat(capabilities.handlesCommand(FIND_COURSE)).isFalse();
        }

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

    }

    @Nested
    class Construction {

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
}
