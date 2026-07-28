/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.framework.messaging.multitenancy.eventhandling.processing;

import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.pooled.MultiTenantPooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;

/**
 * Static factory methods for creating multi-tenant event processor modules.
 * <p>
 * This is the multi-tenant counterpart of {@link EventProcessorModule}. Instead of creating single-instance
 * processors, these factory methods create modules that produce per-tenant event processors backed by a
 * {@link MultiTenantEventProcessor}.
 * <p>
 * Example usage:
 * <pre>{@code
 * var module = MultiTenantEventProcessorModule
 *     .pooledStreaming("order-processor")
 *     .eventHandlingComponents(c -> c.component("orders", cfg -> orderHandler))
 *     .notCustomized();
 * }</pre>
 *
 * @author Jan Galinski
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see EventProcessorModule
 * @see MultiTenantEventProcessor
 */
public final class MultiTenantEventProcessorModule {

    private MultiTenantEventProcessorModule() {
        // utility class
    }

    /**
     * Creates a {@link MultiTenantPooledStreamingEventProcessorModule} with the given name.
     * <p>
     * The returned module creates a {@link MultiTenantEventProcessor} that manages per-tenant
     * {@link PooledStreamingEventProcessor}
     * instances, each reading from its tenant-specific event store and token store.
     *
     * @param name the processor name
     * @return a builder phase to configure event handling components and customization
     */
    public static EventProcessorModule.EventHandlingPhase<MultiTenantPooledStreamingEventProcessorModule, PooledStreamingEventProcessorConfiguration> pooledStreaming(
            String name
    ) {
        return new MultiTenantPooledStreamingEventProcessorModule(name);
    }
}
