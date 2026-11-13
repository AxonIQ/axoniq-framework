/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.util;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * A {@link CommandBusConnector.Handler} for tests, recording the commands handed to it and answering each with
 * whatever a test configured.
 * <p>
 * Stands in for the {@code DistributedCommandBus}'s own handler, which is what a connector invokes for a command
 * routed to its own application.
 *
 * @author Allard Buijze
 */
public class RecordingCommandHandler implements CommandBusConnector.Handler {

    private final List<CommandMessage> handled = new CopyOnWriteArrayList<>();
    private volatile Function<CommandMessage, Outcome> outcome = command -> Outcome.success(null);

    /**
     * Answers every command with the given {@code resultMessage}.
     */
    public RecordingCommandHandler answeringWith(CommandResultMessage resultMessage) {
        this.outcome = command -> Outcome.success(resultMessage);
        return this;
    }

    /**
     * Answers every command by failing with the given {@code cause}.
     */
    public RecordingCommandHandler failingWith(Throwable cause) {
        this.outcome = command -> Outcome.failure(cause);
        return this;
    }

    /**
     * Answers every command with the outcome the given {@code outcome} function decides on.
     */
    public RecordingCommandHandler answering(Function<CommandMessage, Outcome> outcome) {
        this.outcome = outcome;
        return this;
    }

    public List<CommandMessage> handled() {
        return List.copyOf(handled);
    }

    public CommandMessage lastHandled() {
        if (handled.isEmpty()) {
            throw new IllegalStateException("No command was handled.");
        }
        return handled.get(handled.size() - 1);
    }

    @Override
    public void handle(CommandMessage commandMessage, CommandBusConnector.ResultCallback callback) {
        handled.add(commandMessage);
        Outcome decided = outcome.apply(commandMessage);
        if (decided.cause() != null) {
            callback.onError(decided.cause());
        } else {
            callback.onSuccess(decided.resultMessage());
        }
    }

    /**
     * What this handler answers a command with: either a result, possibly absent, or a failure.
     */
    public record Outcome(CommandResultMessage resultMessage, Throwable cause) {

        public static Outcome success(CommandResultMessage resultMessage) {
            return new Outcome(resultMessage, null);
        }

        public static Outcome failure(Throwable cause) {
            return new Outcome(null, cause);
        }
    }
}
