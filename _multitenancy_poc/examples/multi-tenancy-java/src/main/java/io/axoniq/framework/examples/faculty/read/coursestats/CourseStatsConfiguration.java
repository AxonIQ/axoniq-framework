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

package io.axoniq.framework.examples.faculty.read.coursestats;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.eventstreaming.MultiTenantPersistentStreamMessageSource;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.Executors;

public enum CourseStatsConfiguration {
    ;

    public static final Logger logger = LoggerFactory.getLogger(CourseStatsConfiguration.class);

    private static final String PROJECTION_PROCESSOR = "Projection_CourseStats_Processor";
    private static final int SEGMENT_COUNT = 4;
    private static final int BATCH_SIZE = 1024;

    public static EventSourcingConfigurer configure(EventSourcingConfigurer configurer) {
        SubscribingEventProcessorModule projectionProcessor = EventProcessorModule
                .subscribing(PROJECTION_PROCESSOR)
                .eventHandlingComponents(
                        c -> c.autodetected(cfg -> new CoursesStatsProjection())
                )
                .customized((cfg, subscribingConfig) -> {
                    MultiTenantPersistentStreamMessageSource source = buildProjectionSource(cfg);
                    if (cfg.hasComponent(TenantProvider.class)) {
                        cfg.getComponent(TenantProvider.class).subscribe(source);
                    }
                    return subscribingConfig.eventSource(source);
                });

        QueryHandlingModule getCourseStatsByIdQueryHandler = QueryHandlingModule.named("get-course-stats-by-id")
                .queryHandlers()
                .autodetectedQueryHandlingComponent(cfg -> new GetCourseStatsByIdQueryHandler())
                .build();

        QueryHandlingModule getAllCourseStatsQueryHandler = QueryHandlingModule.named("get-all-course-stats")
                .queryHandlers()
                .autodetectedQueryHandlingComponent(cfg -> new GetAllCourseStatsQueryHandler())
                .build();

        return configurer
                .componentRegistry(cr -> {
                    cr.registerComponent(TenantComponentRegistry.class, cfg ->
                            new DefaultTenantComponentRegistry<>(
                                    CourseStatsRepository.class,
                                    tenant -> new InMemoryCourseStatsRepository(tenant.tenantId())
                            )
                    );
                })
                .registerQueryHandlingModule(getCourseStatsByIdQueryHandler)
                .registerQueryHandlingModule(getAllCourseStatsQueryHandler)
                .modelling(modelling -> modelling.messaging(messaging -> messaging.eventProcessing(eventProcessing ->
                        eventProcessing.subscribing(subscribing -> subscribing.processor(projectionProcessor))
                )));
    }

    private static MultiTenantPersistentStreamMessageSource buildProjectionSource(
            org.axonframework.common.configuration.Configuration cfg
    ) {
        return new MultiTenantPersistentStreamMessageSource(
                PROJECTION_PROCESSOR,
                new PersistentStreamProperties(
                        PROJECTION_PROCESSOR,
                        SEGMENT_COUNT,
                        PersistentStreamSequencingPolicy.SEQUENTIAL_PER_AGGREGATE_POLICY,
                        Collections.emptyList(),
                        PersistentStreamProperties.HEAD_POSITION,
                        null
                ),
                Executors.newScheduledThreadPool(SEGMENT_COUNT),
                BATCH_SIZE,
                null,
                cfg,
                (name, persistentStreamProperties, scheduler, batchSize, context, configuration, tenantDescriptor) ->
                        new PersistentStreamMessageSource(
                                name,
                                configuration.getComponent(AxonServerConnectionManager.class),
                                configuration.getComponent(AxonServerConfiguration.class),
                                configuration.getComponent(EventConverter.class),
                                persistentStreamProperties,
                                scheduler,
                                configuration.getComponent(UnitOfWorkFactory.class),
                                batchSize,
                                context
                        )
        );
    }
}
