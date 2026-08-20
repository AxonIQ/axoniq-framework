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

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The workflow properties are the application-facing surface of the {@code axoniq.workflow} prefix, so the binding of
 * every property is asserted here: a renamed prefix or setter still starts the application, silently on the default.
 */
class WorkflowPropertiesTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

    @Test
    void initialSegmentCountBindsFromTheApplicationProperties() {
        contextRunner.withPropertyValues("axoniq.workflow.initial-segment-count=16")
                     .run(context -> assertThat(context.getBean(WorkflowProperties.class).getInitialSegmentCount())
                             .isEqualTo(16));
    }

    @Test
    void initialSegmentCountIsUnsetUnlessTheApplicationConfiguresIt() {
        contextRunner.run(context -> assertThat(context.getBean(WorkflowProperties.class).getInitialSegmentCount())
                .as("""
                    An unset property must stay unset, so the event processing configuration keeps deciding the \
                    segment count. Defaulting it here would silently override whatever that configuration holds.""")
                .isNull());
    }

    @Configuration
    @EnableConfigurationProperties(WorkflowProperties.class)
    static class TestConfiguration {

    }
}
