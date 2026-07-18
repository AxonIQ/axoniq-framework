package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the multi-tenancy feature testing against multi-context Axon Server.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenancyIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
    }

    @AfterEach
    void tearDown() {
        INFRASTRUCTURE.purgeData();
        INFRASTRUCTURE.stop();
    }

    @Test
    void canManageContexts() {
        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder(ADMIN_CONTEXT, DEFAULT_CONTEXT);

        contextManager.createContext("context1");

        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder(ADMIN_CONTEXT, DEFAULT_CONTEXT, "context1");

        contextManager.deleteContext("context1");

        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder(ADMIN_CONTEXT, DEFAULT_CONTEXT);
    }
}
