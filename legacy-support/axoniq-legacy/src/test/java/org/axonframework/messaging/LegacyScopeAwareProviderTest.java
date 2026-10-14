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

package org.axonframework.messaging;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating the {@link LegacyScopeAwareProvider}.
 *
 * @author Jakob Hatzl
 */
class LegacyScopeAwareProviderTest {

    private static final ScopeDescriptor SCOPE = NoScopeDescriptor.INSTANCE;

    @Nested
    class WhenReady {

        @Test
        void providesEveryRegisteredComponent() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofSeconds(5));
            ScopeAware first = new ResolvingScopeAware();
            ScopeAware second = new ResolvingScopeAware();
            provider.register(first);
            provider.register(second);
            provider.markReady();

            // when / then
            assertThat(provider.provideScopeAwareStream(SCOPE)).containsExactly(first, second);
        }

        @Test
        void providesAComponentRegisteredAfterBecomingReady() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofSeconds(5));
            provider.markReady();
            ScopeAware lateComponent = new ResolvingScopeAware();

            // when
            provider.register(lateComponent);

            // then
            assertThat(provider.provideScopeAwareStream(SCOPE)).containsExactly(lateComponent);
        }

        @Test
        void aReleaseAfterBecomingReadyHasNoEffect() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofSeconds(5));
            ScopeAware component = new ResolvingScopeAware();
            provider.register(component);
            provider.markReady();

            // when
            provider.release();

            // then
            assertThat(provider.provideScopeAwareStream(SCOPE)).containsExactly(component);
        }
    }

    @Nested
    class BeforeReady {

        @Test
        void waitsUntilTheProviderBecomesReady() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofSeconds(10));
            ScopeAware component = new ResolvingScopeAware();
            CompletableFuture<Long> provided = CompletableFuture.supplyAsync(
                    () -> provider.provideScopeAwareStream(SCOPE).count()
            );

            // when: the component registers while the caller waits, as a Saga manager does while the event processors
            // start, before the provider becomes ready
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).until(() -> !provided.isDone());
            provider.register(component);
            provider.markReady();

            // then
            assertThat(provided.orTimeout(5, TimeUnit.SECONDS).join()).isEqualTo(1L);
        }

        @Test
        void throwsATransientExceptionWhenNotReadyWithinTheTimeout() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofMillis(50));

            // when / then
            assertThatThrownBy(() -> provider.provideScopeAwareStream(SCOPE))
                    .isInstanceOf(ScopeAwareProviderNotReadyException.class)
                    .hasMessageContaining("did not start within");
        }

        @Test
        void aTimedOutCallerDoesNotFailLaterCallers() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofMillis(50));
            ScopeAware component = new ResolvingScopeAware();
            provider.register(component);
            assertThatThrownBy(() -> provider.provideScopeAwareStream(SCOPE))
                    .isInstanceOf(ScopeAwareProviderNotReadyException.class);

            // when
            provider.markReady();

            // then
            assertThat(provider.provideScopeAwareStream(SCOPE)).containsExactly(component);
        }

        @Test
        void aZeroTimeoutFailsRightAway() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ZERO);

            // when / then
            assertThatThrownBy(() -> provider.provideScopeAwareStream(SCOPE))
                    .isInstanceOf(ScopeAwareProviderNotReadyException.class);
        }

        @Test
        void aReleaseFailsWaitingCallers() {
            // given
            LegacyScopeAwareProvider provider = new LegacyScopeAwareProvider(Duration.ofSeconds(10));
            CompletableFuture<Long> provided = CompletableFuture.supplyAsync(
                    () -> provider.provideScopeAwareStream(SCOPE).count()
            );
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).until(() -> !provided.isDone());

            // when
            provider.release();

            // then
            assertThat(provided).failsWithin(Duration.ofSeconds(5))
                                .withThrowableThat()
                                .havingRootCause()
                                .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> provider.provideScopeAwareStream(SCOPE))
                    .isInstanceOf(ScopeAwareProviderNotReadyException.class)
                    .hasMessageContaining("shut down");
        }
    }

    private static class ResolvingScopeAware implements ScopeAware {

        @Override
        public void send(Message message, ProcessingContext context, ScopeDescriptor scopeDescription) {
        }

        @Override
        public boolean canResolve(ScopeDescriptor scopeDescription) {
            return true;
        }
    }
}
