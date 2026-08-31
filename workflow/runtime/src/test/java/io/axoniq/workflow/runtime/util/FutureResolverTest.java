/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FutureResolver}.
 *
 * @author Simon Zambrovski
 */
class FutureResolverTest {

    @Test
    void defaultResolverWaitsForPublicationToComplete() {
        var resolver = new DefaultTimeoutFutureResolver();
        var publication = CompletableFuture.completedFuture(null);

        resolver.resolve(publication);

        assertThat(publication).isCompleted();
    }

    @Test
    void defaultResolverPropagatesPublicationFailure() {
        var resolver = new DefaultTimeoutFutureResolver();
        var failure = new IllegalStateException("publication failed");

        assertThatThrownBy(() -> resolver.resolve(CompletableFuture.failedFuture(failure)))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(exception -> assertThat(exception).isSameAs(failure));
    }

    @Test
    void defaultResolverTimesOutForIncompletePublication() {
        var resolver = new DefaultTimeoutFutureResolver(Duration.ofMillis(50));
        var publication = new CompletableFuture<Void>();

        assertThatThrownBy(() -> resolver.resolve(publication))
                .isInstanceOf(FutureResolutionTimeoutException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(publication).isNotCompleted();

        publication.complete(null);

        assertThat(publication).isCompletedWithValue(null);
    }

    @Test
    void resolvesPublicationThroughProcessingContextComponent() {
        var context = mock(ProcessingContext.class);
        var resolver = mock(FutureResolver.class);
        var publication = new CompletableFuture<Void>();
        when(context.component(FutureResolver.class)).thenReturn(resolver);

        FutureResolver.resolve(context, publication);

        verify(resolver).resolve(same(publication));
    }

    @Test
    void normalizesRawTimeoutFromConfiguredResolver() {
        var context = mock(ProcessingContext.class);
        FutureResolver resolver = ignored -> throwUnchecked(new TimeoutException("publication timed out"));
        when(context.component(FutureResolver.class)).thenReturn(resolver);

        assertThatThrownBy(() -> FutureResolver.resolve(context, new CompletableFuture<>()))
                .isInstanceOf(FutureResolutionTimeoutException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void resolvesPublicationWhenComponentNotFoundInProcessingContext() {
        var context = mock(ProcessingContext.class);
        var publication = CompletableFuture.completedFuture(null);
        when(context.component(FutureResolver.class)).thenThrow(new ComponentNotFoundException(FutureResolver.class, "name"));

        FutureResolver.resolve(context, publication);

        assertThat(publication).isCompleted();
    }

    @Test
    void resolvesPublicationWhenComponentIsNull() {
        var context = mock(ProcessingContext.class);
        var publication = CompletableFuture.completedFuture(null);
        when(context.component(FutureResolver.class)).thenReturn(null);

        FutureResolver.resolve(context, publication);

        assertThat(publication).isCompleted();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }
}
