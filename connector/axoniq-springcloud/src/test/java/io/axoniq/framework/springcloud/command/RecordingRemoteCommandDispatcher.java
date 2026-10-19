/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.command;

import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link RemoteCommandDispatcher} for tests, recording which member each command was sent to and answering with
 * whatever a test configured.
 * <p>
 * Lets the connector's routing decisions be exercised without a web stack, which is the point of the dispatcher being
 * an interface.
 *
 * @author Allard Buijze
 */
public class RecordingRemoteCommandDispatcher implements RemoteCommandDispatcher {

    private final List<Dispatch> dispatches = new CopyOnWriteArrayList<>();
    private volatile CommandResultMessage resultMessage;
    private volatile Throwable cause;

    public RecordingRemoteCommandDispatcher answeringWith(CommandResultMessage resultMessage) {
        this.resultMessage = resultMessage;
        this.cause = null;
        return this;
    }

    public RecordingRemoteCommandDispatcher failingWith(Throwable cause) {
        this.cause = cause;
        this.resultMessage = null;
        return this;
    }

    public List<Dispatch> dispatches() {
        return List.copyOf(dispatches);
    }

    public List<Member> members() {
        return dispatches.stream().map(Dispatch::member).toList();
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(Member member, CommandMessage command) {
        dispatches.add(new Dispatch(member, command));
        Throwable failure = cause;
        return failure == null
                ? CompletableFuture.completedFuture(resultMessage)
                : CompletableFuture.failedFuture(failure);
    }

    /**
     * One command this dispatcher was asked to send, and the member it was sent to.
     */
    public record Dispatch(Member member, CommandMessage command) {

    }
}
