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

import io.axoniq.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static io.axoniq.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer.DEFAULT_INITIAL_SEGMENT_COUNT;

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
     * Defaults to {@link WorkflowEventProcessingRegistrationEnhancer#DEFAULT_INITIAL_SEGMENT_COUNT}. Only the initial
     * count is set here: once the processor has stored its tokens, the segment count changes through splitting and
     * merging them.
     */
    private int initialSegmentCount = DEFAULT_INITIAL_SEGMENT_COUNT;

    public int getInitialSegmentCount() {
        return initialSegmentCount;
    }

    public void setInitialSegmentCount(int initialSegmentCount) {
        this.initialSegmentCount = initialSegmentCount;
    }
}
