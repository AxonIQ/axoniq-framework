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

package io.axoniq.framework.integrationtests.testsuite.course;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.integrationtests.testsuite.course.commands.CreateCourse;
import org.axonframework.integrationtests.testsuite.course.events.CourseCreated;
import org.axonframework.integrationtests.testsuite.course.module.CreateCourseConfiguration;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

class StudentAxonTestFixtureAxonServerIntegrationIT {

    protected static final Logger logger = LoggerFactory.getLogger(StudentAxonTestFixtureAxonServerIntegrationIT.class);

    /*
     * A context of its own, rather than the shared container's default context, so repeatedly recreating it
     * (once per @BeforeEach, see testConfigurer()) can't collide with other suites sharing the container.
     */
    private static final String CONTEXT = "student-axon-test-fixture-axon-server-it";

    private static final AxonServerContainer container = SharedAxonServerContainer.INSTANCE;

    private AxonTestFixture fixture;

    @BeforeAll
    static void beforeAll() throws IOException {
        SharedAxonServerContainer.ensureStarted();

        try {
            AxonServerContainerUtils.deleteContext(container.getHost(), container.getHttpPort(), CONTEXT);
        } catch (IOException ignored) {
            // Context didn't exist yet.
        }
        AxonServerContainerUtils.createContext(container.getHost(),
                                               container.getHttpPort(),
                                               CONTEXT,
                                               AxonServerContainerUtils.DCB_CONTEXT);
    }

    @BeforeEach
    void setUp() {
        fixture = AxonTestFixture.with(testConfigurer());
    }

    @AfterEach
    void tearDown() {
        fixture.stop();
    }

    private EventSourcingConfigurer testConfigurer() {
        var configurer = EventSourcingConfigurer.create();
        try {
            AxonServerContainerUtils.purgeEventsFromAxonServer(container.getHost(),
                                                               container.getHttpPort(),
                                                               CONTEXT,
                                                               AxonServerContainerUtils.DCB_CONTEXT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Using Axon Server for integration test. UI is available at http://localhost:{}",
                    container.getHttpPort());
        AxonServerConfiguration axonServerConfiguration = new AxonServerConfiguration();
        axonServerConfiguration.setServers(container.getHost() + ":" + container.getGrpcPort());
        axonServerConfiguration.setContext(CONTEXT);
        configurer.componentRegistry(cr -> cr.registerComponent(
                AxonServerConfiguration.class,
                c -> axonServerConfiguration
        ));
        return CreateCourseConfiguration.configure(configurer);
    }

    @RepeatedTest(5)
    void axonTestFixtureWorksWithAxonServer() {
        var courseId = UUID.randomUUID().toString();

        fixture.given()
               .when()
               .command(new CreateCourse(courseId))
               .then()
               .success()
               .events(new CourseCreated(courseId));
    }

    @RepeatedTest(5)
    void axonTestFixtureWorksWithAxonServerShutdown() {
        var courseId = UUID.randomUUID().toString();

        fixture.given()
               .when()
               .command(new CreateCourse(courseId))
               .then()
               .success()
               .events(new CourseCreated(courseId));
    }
}
