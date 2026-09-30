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

package io.axoniq.framework.springcloud.transport;

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
