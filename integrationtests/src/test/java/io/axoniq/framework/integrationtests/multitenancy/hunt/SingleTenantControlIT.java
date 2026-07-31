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

package io.axoniq.framework.integrationtests.multitenancy.hunt;

import io.axoniq.framework.integrationtests.multitenancy.DisableMultiTenancyTestsWithoutLicense;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.Account;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.AccountOpened;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.RecordBalance;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The single-tenant control arm of {@link MultiTenantReadYourWritesIT}: the SAME entity, events and command flow on
 * the same Axon Server, default context, WITHOUT multi-tenancy. Splits the read-your-writes verdict: green here plus
 * red there attributes the loss to the multi-tenant assembly; red here indicts the shared domain wiring or the
 * branch's storage path as a whole.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class SingleTenantControlIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "control-account";
    private static final Map<String, String> CONTROL_NOTES = new ConcurrentHashMap<>();

    private AxonConfiguration application;

    static final class ControlHandlers {

        @CommandHandler
        void handle(OpenAccount command,
                    @InjectEntity(idProperty = "accountId") Account state,
                    EventAppender appender) {
            appender.append(new AccountOpened(command.accountId()));
        }

        @CommandHandler
        void handle(TenantBankFixture.DepositMoney command,
                    @InjectEntity(idProperty = "accountId") Account state,
                    EventAppender appender) {
            appender.append(new TenantBankFixture.MoneyDeposited(command.accountId(), command.amount()));
        }

        @CommandHandler
        void handle(RecordBalance command,
                    @InjectEntity(idProperty = "accountId") Account state) {
            CONTROL_NOTES.put("open-" + command.accountId(), Boolean.toString(state.isOpen()));
        }
    }

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        CONTROL_NOTES.clear();
        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, Account.class))
                  .registerCommandHandlingModule(
                          CommandHandlingModule.named("control-commands")
                                               .commandHandlers()
                                               .autodetectedCommandHandlingComponent(c -> new ControlHandlers()))
                  .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                  .componentRegistry(registry -> registry
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .disableEnhancer(io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSourceConfigurationEnhancer.class));
        application = configurer.start();
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        INFRASTRUCTURE.stop();
    }

    @Test
    void acknowledgedAppendIsSourcedByTheNextCommandOnTheDefaultContext() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        gateway.send(new OpenAccount(ACCOUNT_ID), Metadata.emptyInstance(), null)
               .getResultMessage().orTimeout(30, TimeUnit.SECONDS).join();
        gateway.send(new RecordBalance(ACCOUNT_ID), Metadata.emptyInstance(), null)
               .getResultMessage().orTimeout(30, TimeUnit.SECONDS).join();

        assertThat(CONTROL_NOTES.get("open-" + ACCOUNT_ID))
                .as("single-tenant control: the default-context flow must source its own acknowledged append")
                .isEqualTo("true");
    }
}
