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
package io.axoniq.workflow.springboot;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The workflow properties exposed to Spring Boot applications under the {@code axoniq.workflow} prefix.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@ConfigurationProperties(prefix = "axoniq.workflow")
public class WorkflowProperties {

    /**
     * The number of segments to initialize the workflow event processor with. Workflow instances are partitioned over
     * segments by workflow id, so this decides how far the instances of one application can spread over its nodes.
     * <p>
     * Unset by default, leaving the count to the event processing configuration. Only the initial count is set here:
     * once the processor has stored its tokens, the segment count changes through splitting and merging them.
     * <p>
     * Setting this above 1 only spreads instances across nodes when the application also registers a durable
     * {@code TokenStore}. Without one, the processor falls back to an in-memory token store and every claim stays
     * process-local, so multiple segments run on the same node without actually distributing any load.
     */
    @Nullable
    private Integer initialSegmentCount;

    @Nullable
    public Integer getInitialSegmentCount() {
        return initialSegmentCount;
    }

    public void setInitialSegmentCount(@Nullable Integer initialSegmentCount) {
        this.initialSegmentCount = initialSegmentCount;
    }
}
