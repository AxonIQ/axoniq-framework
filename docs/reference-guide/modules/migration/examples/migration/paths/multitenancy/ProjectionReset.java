/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package migration.paths.multitenancy;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Resetting a projection in a multi-tenant application, where one processor serves every tenant.
 */
public class ProjectionReset {

    private final Configuration configuration;

    public ProjectionReset(Configuration configuration) {
        this.configuration = configuration;
    }

    // tag::reset-projection[]
    public CompletableFuture<Void> reset(String processingGroup) {
        StreamingEventProcessor processor =
                Optional.ofNullable(configuration.getComponents(StreamingEventProcessor.class)
                                                 .get(processingGroup))                  // <1>
                        .orElseThrow(() -> new IllegalArgumentException(
                                "No streaming event processor named [" + processingGroup + "]"));
        return processor.shutdown()
                        .thenCompose(result -> processor.resetTokens())                  // <2>
                        .thenCompose(result -> processor.start());
    }
    // end::reset-projection[]
}
