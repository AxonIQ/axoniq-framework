/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantsequencing;

import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.api.TenantSequencingPolicy;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurationDefaults;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;

public class TenantSequencingConfiguration {

    // tag::command-sequencing[]
    public void configureCommandSequencing(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                SequencingPolicy.class,
                MessagingConfigurationDefaults.COMMAND_SEQUENCING_POLICY,
                configuration -> TenantSequencingPolicy.from(configuration.getComponent(TenantRouter.class))
        ));
    }
    // end::command-sequencing[]

    // tag::event-sequencing[]
    public void configureEventSequencing(MessagingConfigurer configurer) {
        configurer.eventProcessing(eventProcessing -> eventProcessing.pooledStreaming(
                pooled -> pooled.processor(
                        EventProcessorModule.pooledStreaming("tenant-aware-projection")
                                            .eventHandlingComponents(components -> components.declarative(
                                                    "tenant-aware-projection",
                                                    configuration -> {
                                                        var component = SimpleEventHandlingComponent.create(
                                                                "tenant-aware-projection",
                                                                TenantSequencingPolicy.from(
                                                                        configuration.getComponent(TenantRouter.class)
                                                                )
                                                        );
                                                        component.subscribe(
                                                                new QualifiedName("example", "CourseUpdated"),
                                                                (event, context) -> MessageStream.empty()
                                                        );
                                                        return component;
                                                    }
                                            ))
                                            .notCustomized()
                )
        ));
    }
    // end::event-sequencing[]
}
