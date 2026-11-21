/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.util;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link WorkflowIds}.
 *
 * @author Allard Buijze
 */
class WorkflowIdsTest {

    @Nested
    class Invalidity {

        @Test
        void rejectsAnEmptyId() {
            // when
            var invalidity = WorkflowIds.invalidity("");

            // then
            assertThat(invalidity).hasValueSatisfying(reason -> assertThat(reason).contains("empty"));
            assertThat(WorkflowIds.isValid("")).isFalse();
        }

        @Test
        void acceptsAnIdOfOneCharacter() {
            // given
            var letter = "w";
            var space = " ";

            // when / then
            assertThat(WorkflowIds.invalidity(letter)).isEmpty();
            assertThat(WorkflowIds.invalidity(space)).isEmpty();
        }

        @Test
        void acceptsAnIdOfTheMaximumLength() {
            // given
            var longest = "w".repeat(255);

            // when / then
            assertThat(WorkflowIds.invalidity(longest)).isEmpty();
            assertThat(WorkflowIds.isValid(longest)).isTrue();
        }

        @Test
        void rejectsAnIdLongerThanTheMaximumLength() {
            // given
            var tooLong = "w".repeat(256);

            // when
            var invalidity = WorkflowIds.invalidity(tooLong);

            // then
            assertThat(invalidity).hasValueSatisfying(reason -> assertThat(reason).contains("256", "255"));
            assertThat(WorkflowIds.isValid(tooLong)).isFalse();
        }

        @Test
        void acceptsEveryPrintableAsciiCharacter() {
            // given - the space up to the tilde, which includes the first and last printable ASCII characters
            var builder = new StringBuilder();
            for (char c = 0x20; c <= 0x7E; c++) {
                builder.append(c);
            }
            var printable = builder.toString();

            // when / then
            assertThat(printable).startsWith(" ").endsWith("~").hasSize(95);
            assertThat(WorkflowIds.invalidity(printable)).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"order\u0000", "order\t1", "order\n1", "order\u001F", "order\u007F", "bestellung-ü",
                "order-€", "order-😀"})
        void rejectsControlAndNonAsciiCharacters(String workflowId) {
            // when
            var invalidity = WorkflowIds.invalidity(workflowId);

            // then
            assertThat(invalidity).hasValueSatisfying(reason -> assertThat(reason).contains("not printable ASCII"));
        }

        @Test
        void namesTheIndexAndCodePointOfTheFirstInvalidCharacter() {
            // when
            var invalidity = WorkflowIds.invalidity("order-ü\t");

            // then
            assertThat(invalidity).hasValueSatisfying(reason -> assertThat(reason).contains("index 6", "U+00FC"));
        }

        @Test
        void namesTheFullCodePointOfACharacterOutsideTheBasicMultilingualPlane() {
            // when
            var invalidity = WorkflowIds.invalidity("order-\uD83D\uDE00");

            // then
            assertThat(invalidity).hasValueSatisfying(reason -> assertThat(reason).contains("index 6", "U+1F600"));
        }

        @Test
        void rejectsNull() {
            assertThatThrownBy(() -> WorkflowIds.invalidity(null)).isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Abbreviate {

        @Test
        void keepsAShortIdAsItIs() {
            assertThat(WorkflowIds.abbreviate("order-1")).isEqualTo("order-1");
        }

        @Test
        void keepsTheFirst64CharactersOfALongerId() {
            // given
            var longId = "a".repeat(64) + "b".repeat(1_000);

            // when / then
            assertThat(WorkflowIds.abbreviate(longId)).isEqualTo("a".repeat(64) + "...");
            assertThat(WorkflowIds.abbreviate("a".repeat(64))).isEqualTo("a".repeat(64));
        }

        @Test
        void escapesCharactersThatAreNotPrintableAscii() {
            assertThat(WorkflowIds.abbreviate("order-ü\n")).isEqualTo("order-\\u00FC\\u000A");
        }

        @Test
        void escapesASurrogatePairAsTheCodePointItEncodes() {
            assertThat(WorkflowIds.abbreviate("order-\uD83D\uDE00")).isEqualTo("order-\\u1F600");
        }
    }
}
