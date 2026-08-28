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

package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.TenantDescriptorMapping;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves per-tenant event storage isolates an event-sourced entity end to end: a command carrying its tenant in
 * metadata resolves that tenant onto the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}, the
 * entity is sourced through the {@link MultiTenantEventStorageEngine}, and its events are appended back through the same
 * engine, all routed to that one tenant's store.
 * <p>
 * The engine's routing given a resolved tenant is covered by
 * {@link io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngineTest}, and the Axon
 * Server wiring that registers the engine by {@code AxonServerMultiTenancyConfigurationDefaultsTest}. This
 * test closes the gap between them by driving a real {@link CommandGateway} through the whole write path: a course
 * filled in one tenant must not make the same course identifier appear full in another. Each tenant is backed by its
 * own {@link InMemoryEventStorageEngine}, so a routing leak would source another tenant's events and fail the test.
 */
class PerTenantEventStorageIsolationTest {

    private static final String COURSE_ID_TAG = "courseId";
    private static final String SHARED_COURSE_ID = "cs-101";
    private static final int COMMAND_TIMEOUT_SECONDS = 5;

    private final TenantDescriptorMapping<EventStorageEngine> tenantEngines = new TenantDescriptorMapping<>();
    private final TenantDescriptorMapping<SnapshotStore> tenantSnapshotStores = new TenantDescriptorMapping<>();

    private AxonConfiguration configuration;
    private CommandGateway commandGateway;

    @BeforeEach
    void setUp() {
        tenantEngines.entry(TENANT_A, new InMemoryEventStorageEngine());
        tenantEngines.entry(TENANT_B, new InMemoryEventStorageEngine());
        tenantSnapshotStores.entry(TENANT_A, new RecordingSnapshotStore());
        tenantSnapshotStores.entry(TENANT_B, new RecordingSnapshotStore());
        MetadataBasedTenantResolver tenantResolver = new MetadataBasedTenantResolver();
        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_B);

        // Subscribed rather than registered by hand, so the tenants reach the engine the way they do in production.
        MultiTenantEventStorageEngine routingEngine = new MultiTenantEventStorageEngine(
                tenantEngines::apply,
                tenantSnapshotStores::apply,
                new TenantRouter(tenantResolver, tenantEngines));
        tenantProvider.subscribe(routingEngine);

        configuration = EventSourcingConfigurer
                .create()
                .registerEntity(EventSourcedEntityModule.autodetected(String.class, Course.class))
                .registerCommandHandlingModule(
                        CommandHandlingModule.named("course")
                                             .commandHandlers()
                                             .autodetectedCommandHandlingComponent(config -> new CourseCommandHandler()))
                .componentRegistry(registry -> {
                    // Keep the whole flow in memory: no Axon Server command bus or event store, so the routing engine
                    // registered below is the one the command handling exercises.
                    registry.disableEnhancer(AxonServerConfigurationEnhancer.class)
                            .disableEnhancer(AxonServerMultiTenancyConfigurationDefaults.class)
                            .registerComponent(TenantProvider.class, config -> tenantProvider)
                            .registerComponent(EventStorageEngine.class, config -> routingEngine);
                })
                .build();
        configuration.start();
        commandGateway = configuration.getComponent(CommandGateway.class);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void aFullCourseInOneTenantDoesNotFillTheSameCourseIdInAnother() {
        enroll(TENANT_A, "alice");

        assertThatThrownBy(() -> enroll(TENANT_A, "bob"))
                .hasRootCauseInstanceOf(CourseFullException.class);

        assertThatCode(() -> enroll(TENANT_B, "carol"))
                .doesNotThrowAnyException();
    }

    private void enroll(TenantDescriptor tenant, String studentId) {
        commandGateway.send(new EnrollStudent(SHARED_COURSE_ID, studentId),
                            Metadata.with(DEFAULT_TENANT_METADATA_KEY, tenant.tenantId()),
                            null)
                      .getResultMessage()
                      .orTimeout(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                      .join();
    }

    private record EnrollStudent(String courseId, String studentId) {

    }

    private record StudentEnrolledInCourse(@EventTag(key = COURSE_ID_TAG) String courseId, String studentId) {

    }

    private static final class CourseFullException extends RuntimeException {

        private CourseFullException(String courseId) {
            super("Course [" + courseId + "] is full");
        }
    }

    @EventSourcedEntity(tagKey = COURSE_ID_TAG)
    static class Course {

        private static final int CAPACITY = 1;

        private int seatsTaken;

        @EntityCreator
        Course() {
            // A fresh course, evolved from its own tenant's events before the command handler sees it.
        }

        @EventSourcingHandler
        void evolve(StudentEnrolledInCourse event) {
            seatsTaken++;
        }

        private boolean isFull() {
            return seatsTaken >= CAPACITY;
        }
    }

    static class CourseCommandHandler {

        @CommandHandler
        void handle(EnrollStudent command,
                    @Nullable @InjectEntity(idProperty = COURSE_ID_TAG) Course course,
                    EventAppender eventAppender) {
            if (course != null && course.isFull()) {
                throw new CourseFullException(command.courseId());
            }
            eventAppender.append(new StudentEnrolledInCourse(command.courseId(), command.studentId()));
        }
    }
}
