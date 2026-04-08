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

package org.axonframework.test.aggregate;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.modelling.command.Aggregate;
import org.axonframework.modelling.command.Repository;

/**
 * @author Allard Buijze
 */
class MyCommandHandler {

    private Repository<StandardAggregate> repository;
    private EventBus eventBus;

    MyCommandHandler(Repository<StandardAggregate> repository, EventBus eventBus) {
        this.repository = repository;
        this.eventBus = eventBus;
    }

    MyCommandHandler() {
    }

    @CommandHandler
    public void createAggregate(CreateAggregateCommand command) throws Exception {
        repository.newInstance(() -> new StandardAggregate(0, command.getAggregateIdentifier()));
    }

    @CommandHandler
    public void handleTestCommand(TestCommand testCommand) {
        repository.load(testCommand.getAggregateIdentifier().toString())
                  .execute(StandardAggregate::doSomething);
    }

    @CommandHandler
    public void handleStrangeCommand(StrangeCommand testCommand) {
        repository.load(testCommand.getAggregateIdentifier().toString()).execute(StandardAggregate::doSomething);
        eventBus.publish(null, new GenericEventMessage(
                new MessageType("event"), new MyApplicationEvent()
        ));
        throw new StrangeCommandReceivedException("Strange command received");
    }

    @CommandHandler
    public void handleEventPublishingCommand(PublishEventCommand testCommand) {
        eventBus.publish(null, new GenericEventMessage(
                new MessageType("event"), new MyApplicationEvent()
        ));
    }

    @CommandHandler
    public void handleIllegalStateChange(IllegalStateChangeCommand command) {
        Aggregate<StandardAggregate> aggregate = repository.load(command.getAggregateIdentifier().toString());
        aggregate.execute(r -> r.doSomethingIllegal(command.getNewIllegalValue()));
    }

    @CommandHandler
    public void handleDeleteAggregate(DeleteCommand command) {
        repository.load(command.getAggregateIdentifier().toString())
                  .execute(r -> r.delete(command.isAsIllegalChange()));
    }

    public void setRepository(Repository<StandardAggregate> repository) {
        this.repository = repository;
    }
}
