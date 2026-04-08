/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.integrationtests.cache;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.CreationPolicy;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * @author Allard Buijze
 */
public class TestAggregateRoot {

    @AggregateIdentifier
    private String id;

    TestAggregateRoot() {
    }

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    public void handle(CreateCommand cmd) {
        apply(new CreatedEvent(cmd.id));
    }

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    public void handle(FailingCreateCommand cmd) {
        throw new IllegalArgumentException("I don't like this");
    }

    @CommandHandler
    public void throwException(FailCommand cmd) {
        throw new IllegalArgumentException("I don't like this");
    }

    @EventHandler
    public void onMessage(CreatedEvent event) {
        this.id = event.id;
    }

    public static class CreateCommand {

        @TargetAggregateIdentifier
        private final String id;

        public CreateCommand(String id) {
            this.id = id;
        }
    }

    public static class FailingCreateCommand {

        @TargetAggregateIdentifier
        private final String id;

        public FailingCreateCommand(String id) {
            this.id = id;
        }
    }

    public static class FailCommand {

        @TargetAggregateIdentifier
        private final String id;

        public FailCommand(String id) {
            this.id = id;
        }
    }

    public static class CreatedEvent {

        private final String id;

        public CreatedEvent(String id) {
            this.id = id;
        }
    }
}
