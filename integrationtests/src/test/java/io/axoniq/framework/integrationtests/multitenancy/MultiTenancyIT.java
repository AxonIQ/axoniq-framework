package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the multi-tenancy feature testing against multi-context Axon Server.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 5.3.0
 */
public class MultiTenancyIT {

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
        Assumptions.assumeTrue(AxonServerTestInfrastructure.licenseExists());
        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder("_admin", "default");

        contextManager.createContext("context1");

        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder("_admin", "default", "context1");

        contextManager.deleteContext("context1");

        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder("_admin", "default");
    }
}
