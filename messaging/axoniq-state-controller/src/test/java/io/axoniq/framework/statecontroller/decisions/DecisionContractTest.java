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

package io.axoniq.framework.statecontroller.decisions;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Pins the immediate value-level contracts on the {@link Decision} value type so they don't drift: null-handling
 * on the factories, the {@link Decision.Accept#returning(Object) returning} round-trip, and the
 * <em>replace, not append</em> semantics of {@link Decision.Reject#recording(Object...)}.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
class DecisionContractTest {

    @Nested
    class EmitFactory {

        @Test
        void emitWithoutEventsYieldsAnEmptyAcceptCarryingNoResult() {
            // when
            Decision.Accept accept = Decision.emit();

            // then
            assertThat(accept.events()).isEmpty();
            assertThat(accept.result()).isNull();
        }

        @Test
        void emitWithEventsCapturesThemInDeclaredOrder() {
            // given
            Object e1 = new Object();
            Object e2 = new Object();

            // when
            Decision.Accept accept = Decision.emit(e1, e2);

            // then
            assertThat(accept.events()).containsExactly(e1, e2);
        }

        @Test
        void emitRejectsANullVarargsArray() {
            assertThatNullPointerException().isThrownBy(() -> Decision.emit((Object[]) null));
        }

        @Test
        void returningAttachesAResultToAnExistingAccept() {
            // given
            Decision.Accept base = Decision.emit("event");

            // when
            Decision.Accept withResult = base.returning("R-1");

            // then
            assertThat(withResult.events()).containsExactly("event");
            assertThat(withResult.result()).isEqualTo("R-1");
            // and the original is unaffected (records are immutable)
            assertThat(base.result()).isNull();
        }

        @Test
        void returningCanClearAPreviouslySetResultByPassingNull() {
            // given
            Decision.Accept withResult = Decision.emit("event").returning("R-1");

            // when
            Decision.Accept cleared = withResult.returning(null);

            // then
            assertThat(cleared.result()).isNull();
        }
    }

    @Nested
    class RejectFactory {

        @Test
        void rejectCarriesTheReasonAndAnEmptyAuditTrailByDefault() {
            // when
            Decision.Reject reject = Decision.reject("nope");

            // then
            assertThat(reject.reason()).isEqualTo("nope");
            assertThat(reject.auditEvents()).isEmpty();
        }

        @Test
        void recordingReplacesAnyPreviouslyRecordedAuditEvents() {
            // given — first recording attaches one event
            Decision.Reject first = Decision.reject("denied").recording("audit-1");
            assertThat(first.auditEvents()).containsExactly("audit-1");

            // when — recording a second time
            Decision.Reject second = first.recording("audit-2", "audit-3");

            // then — the previous trail is dropped, not concatenated
            assertThat(second.auditEvents()).containsExactly("audit-2", "audit-3");
            // and — the reason is preserved
            assertThat(second.reason()).isEqualTo("denied");
        }

        @Test
        void recordingRejectsANullVarargsArray() {
            // given
            Decision.Reject reject = Decision.reject("nope");

            // when / then
            assertThatNullPointerException().isThrownBy(() -> reject.recording((Object[]) null));
        }
    }
}
