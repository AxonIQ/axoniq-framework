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
package io.axoniq.workflow.runtime.test.configuration;

import io.axoniq.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.workflow.runtime.test.utils.IdGenerator;
import io.axoniq.workflow.runtime.test.utils.RecordingIdGenerator;
import io.axoniq.workflow.runtime.test.utils.TestEventPublisher;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Clock;
import java.util.concurrent.Executor;

/**
 * Test configuration enhancer that installs workflow test event-publication components.
 *
 * <p>This enhancer adds the supporting infrastructure used by workflow tests that need to publish events through the
 * same event pipeline as the runtime while keeping publication deterministic and observable. It registers three
 * components:</p>
 * <ul>
 *     <li>a {@link RecordingIdGenerator} as the test {@link IdGenerator}, so generated event ids can be inspected</li>
 *     <li>a {@link TestEventPublisher}, which publishes payloads or event messages through the configured
 *     {@link EventSink} using the active {@link MessageTypeResolver}, {@link EventConverter}, clock, and that
 *     generator</li>
 *     <li>a {@link DelayedPublisher}, which can schedule those test events on the configured workflow executor</li>
 * </ul>
 *
 * <p>Use this enhancer when a test should drive workflows by publishing real events, especially when the test also
 * needs access to generated ids or delayed event publication. It is commonly used by higher-level workflow test
 * infrastructure and can also be registered directly in custom test configurations.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowTestEventPublicationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry
                .registerComponent(IdGenerator.class, cfg -> new RecordingIdGenerator() {
                })
                .registerComponent(TestEventPublisher.class, cfg -> new TestEventPublisher(
                        cfg.getComponent(EventSink.class),
                        cfg.getComponent(MessageTypeResolver.class),
                        cfg.getComponent(EventConverter.class),
                        cfg.getComponent(Clock.class),
                        cfg.getComponent(IdGenerator.class)
                ))
                .registerComponent(DelayedPublisher.class, cfg ->
                        new DelayedPublisher(
                                cfg.getComponent(TestEventPublisher.class),
                                cfg.getComponent(Executor.class,
                                                 WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR)
                        )
                );
    }
}
