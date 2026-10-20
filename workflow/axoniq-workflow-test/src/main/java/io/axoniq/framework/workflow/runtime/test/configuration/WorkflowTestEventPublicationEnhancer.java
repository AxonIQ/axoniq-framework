/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.configuration;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.framework.workflow.runtime.test.utils.IdGenerator;
import io.axoniq.framework.workflow.runtime.test.utils.RecordingIdGenerator;
import io.axoniq.framework.workflow.runtime.test.utils.TestEventPublisher;
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
