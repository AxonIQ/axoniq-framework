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

package io.axoniq.framework.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for the Micrometer tracing thread-local context-propagation bridge.
 * <p>
 * Binds {@code axon.tracing.thread-local-context-propagation.*}, controlling whether the active trace context (and other
 * bridged thread-bound state) is restored on the framework's worker threads through the {@code UnitOfWork}
 * action-interceptor seam. This governs the in-process thread-local bridge only; cross-service trace-context
 * propagation (via the {@code Propagator} over message metadata) is part of core tracing and is unaffected:
 * <pre>{@code
 * axon:
 *   tracing:
 *     thread-local-context-propagation:
 *       enabled: true
 * }</pre>
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@ConfigurationProperties(prefix = "axon.tracing.thread-local-context-propagation")
public class ThreadLocalContextPropagationProperties {

    /**
     * Whether the Micrometer thread-local context-propagation bridge is enabled. Defaults to {@code true}.
     */
    private boolean enabled = true;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
