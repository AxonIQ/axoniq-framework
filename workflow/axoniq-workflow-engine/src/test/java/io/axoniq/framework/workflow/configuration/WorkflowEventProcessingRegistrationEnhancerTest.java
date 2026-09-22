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
package io.axoniq.framework.workflow.configuration;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Workflow events append under a condition only an event store transaction carries, so the engine refuses to start on a
 * sink that is no event store.
 */
class WorkflowEventProcessingRegistrationEnhancerTest {

    @Test
    void aConfigurationWithoutAnEventStoreRefusesToStart() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var config = mock(Configuration.class);
        when(config.getOptionalComponent(EventStore.class)).thenReturn(Optional.empty());
        assertThatThrownBy(() -> enhancer.requireEventStore(config))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("requires an EventStore");
    }

    @Test
    void anEventStoreConfigurationStarts() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var config = mock(Configuration.class);
        when(config.getOptionalComponent(EventStore.class)).thenReturn(Optional.of(mock(EventStore.class)));

        assertThatCode(() -> enhancer.requireEventStore(config)).doesNotThrowAnyException();
    }
}
