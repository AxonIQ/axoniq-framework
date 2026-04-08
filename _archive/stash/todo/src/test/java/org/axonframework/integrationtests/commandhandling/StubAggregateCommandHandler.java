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

package org.axonframework.integrationtests.commandhandling;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.modelling.command.Aggregate;
import org.axonframework.modelling.command.Repository;

class StubAggregateCommandHandler {

    private Repository<StubAggregate> repository;
    private EventBus eventBus;

    @CommandHandler
    public void handleStubAggregateCreated(CreateStubAggregateCommand command) throws Exception {
        repository.newInstance(() -> {
            StubAggregate aggregate = new StubAggregate();
            aggregate.handle(command.getAggregateId());
            return aggregate;
        });
    }

    @CommandHandler
    public void handleStubAggregateUpdated(UpdateStubAggregateCommand command) {
        repository.load(command.getAggregateId().toString())
                  .execute(StubAggregate::makeAChange);
    }

    @CommandHandler
    public void handleStubAggregateUpdatedWithExtraEvent(UpdateStubAggregateWithExtraEventCommand command) {
        Aggregate<StubAggregate> aggregate = repository.load(command.getAggregateId().toString());
        aggregate.execute(StubAggregate::makeAChange);
        eventBus.publish(null, new GenericEventMessage(new MessageType("event"), new MyEvent()));
        aggregate.execute(StubAggregate::makeAChange);
    }

    @CommandHandler
    public void handleStubAggregateLooping(LoopingCommand command) {
        repository.load(command.getAggregateId().toString())
                  .execute(StubAggregate::makeALoopingChange);
    }

    @CommandHandler
    public void handleProblematicCommand(ProblematicCommand command) {
        repository.load(command.getAggregateId().toString())
                  .execute(StubAggregate::causeTrouble);
    }

    public void setRepository(Repository<StubAggregate> repository) {
        this.repository = repository;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }
}
