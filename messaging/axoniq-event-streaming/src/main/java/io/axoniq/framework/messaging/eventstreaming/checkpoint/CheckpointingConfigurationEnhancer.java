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

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule.Customization;

/**
 * A {@link ConfigurationEnhancer} that enables self-checkpointing support for
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor pooled
 * streaming event processors}.
 * <p>
 * The enhancer installs {@link CheckpointingProgressStrategyFactory#detecting()} as the progress-strategy factory
 * builder on every pooled streaming processor, through the type-level
 * {@link PooledStreamingEventProcessorModule.Customization} hook. As a result, processors whose event handling
 * components implement {@link Checkpointing} automatically defer progress to the components' checkpoint requests,
 * while processors without such components keep the default token-storing behavior.
 * <p>
 * The checkpointing customization is applied <em>before</em> any user-supplied customization: an application-level
 * {@link PooledStreamingEventProcessorModule.Customization} component is preserved and composed after it, and
 * per-processor customizations (via
 * {@link PooledStreamingEventProcessorModule#customized(java.util.function.BiFunction)}) run last. Either can
 * override the
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration#progressStrategyFactoryBuilder(java.util.function.Function)
 * progress-strategy factory builder} to opt out or refine the selection.
 * <p>
 * This enhancer is discovered through the {@link java.util.ServiceLoader} mechanism; having this module on the
 * classpath is sufficient to activate checkpointing detection.
 *
 * @author Allard Buijze
 * @since 5.3.0
 */
public class CheckpointingConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(Customization.class)) {
            registry.registerComponent(Customization.class, config -> Customization.noOp());
        }
        registry.registerDecorator(
                DecoratorDefinition.forType(Customization.class)
                                   .with((config, name, delegate) ->
                                                 checkpointingCustomization().andThen(delegate))
        );
    }

    /**
     * Returns the customization installing {@link CheckpointingProgressStrategyFactory#detecting() checkpointing
     * detection} as the progress-strategy factory builder.
     *
     * @return the customization enabling self-checkpointing detection on a pooled streaming processor
     */
    private static Customization checkpointingCustomization() {
        return (config, processorConfig) ->
                processorConfig.progressStrategyFactoryBuilder(CheckpointingProgressStrategyFactory.detecting());
    }
}
