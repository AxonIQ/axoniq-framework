/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.util;

import io.axoniq.framework.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link FutureResolver}.
 *
 * @author Simon Zambrovski
 */
class FutureResolverTest {

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }

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
        when(context.component(FutureResolver.class)).thenThrow(new ComponentNotFoundException(FutureResolver.class,
                                                                                               "name"));
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
}
