/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *    https://www.axoniq.io/pricing
 */

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.configuration.ModuleBuilder;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.Snapshotting;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Shared snapshotted ledger fixture for the snapshot sourcing integration tests. */
public final class TenantLedgerFixture {

    private static final Map<String, Long> OBSERVED_TOTALS = new ConcurrentHashMap<>();

    private TenantLedgerFixture() {
    }

    public static Map<String, Long> observedTotals() {
        return OBSERVED_TOTALS;
    }

    public static ModuleBuilder<CommandHandlingModule> commandModule() {
        return CommandHandlingModule.named("ledger-commands")
                                    .commandHandlers()
                                    .autodetectedCommandHandlingComponent(c -> new LedgerHandlers());
    }

    public record OpenLedger(String ledgerId) {

    }

    public record AddAmount(String ledgerId, long amount) {

    }

    public record ReportTotal(String ledgerId) {

    }

    public record LedgerOpened(@EventTag(key = "ledgerId") String ledgerId) {

    }

    public record AmountAdded(@EventTag(key = "ledgerId") String ledgerId, long amount) {

    }

    @EventSourcedEntity(tagKey = "ledgerId")
    @Snapshotting(afterEvents = 3)
    public static final class Ledger {

        private long total;

        @EntityCreator
        public Ledger() {
        }

        @EventSourcingHandler
        void evolve(LedgerOpened event) {
            // Opening carries no amount.
        }

        @EventSourcingHandler
        void evolve(AmountAdded event) {
            total += event.amount();
        }

        public long getTotal() {
            return total;
        }
    }

    private static final class LedgerHandlers {

        @CommandHandler
        void handle(OpenLedger command,
                    @InjectEntity(idProperty = "ledgerId") Optional<Ledger> state,
                    EventAppender appender) {
            if (state.isEmpty()) {
                appender.append(new LedgerOpened(command.ledgerId()));
            }
        }

        @CommandHandler
        void handle(AddAmount command,
                    @InjectEntity(idProperty = "ledgerId") Ledger state,
                    EventAppender appender) {
            appender.append(new AmountAdded(command.ledgerId(), command.amount()));
        }

        @CommandHandler
        void handle(ReportTotal command,
                    @InjectEntity(idProperty = "ledgerId") Ledger state,
                    ProcessingContext context) {
            String key = TenantDescriptor.fromContext(context)
                                         .map(TenantDescriptor::tenantId)
                                         .orElse("<single-tenant>");
            OBSERVED_TOTALS.put(key, state.total);
        }
    }
}
