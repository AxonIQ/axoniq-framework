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
