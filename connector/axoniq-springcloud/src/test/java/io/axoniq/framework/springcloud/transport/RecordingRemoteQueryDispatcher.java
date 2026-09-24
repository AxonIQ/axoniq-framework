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
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link RemoteQueryDispatcher} recording what it was asked to send, answering with what it was told to.
 *
 * @author Allard Buijze
 */
public class RecordingRemoteQueryDispatcher implements RemoteQueryDispatcher {

    private final List<Dispatch> dispatches = new CopyOnWriteArrayList<>();

    private volatile List<QueryResponseMessage> responses = List.of();
    private volatile Throwable cause;
    private volatile MessageStream<QueryResponseMessage> stream;

    public RecordingRemoteQueryDispatcher answeringWith(QueryResponseMessage... responses) {
        this.responses = List.of(responses);
        this.cause = null;
        return this;
    }

    /**
     * Answers with the given {@code stream}, so that a query can be left open for as long as a test needs it to be.
     */
    public RecordingRemoteQueryDispatcher answeringWith(MessageStream<QueryResponseMessage> stream) {
        this.stream = stream;
        this.cause = null;
        return this;
    }

    public RecordingRemoteQueryDispatcher failingWith(Throwable cause) {
        this.cause = cause;
        return this;
    }

    public List<Dispatch> dispatches() {
        return List.copyOf(dispatches);
    }

    public List<Member> members() {
        return dispatches.stream().map(Dispatch::member).toList();
    }

    @Override
    public MessageStream<QueryResponseMessage> dispatch(Member member, QueryMessage query) {
        dispatches.add(new Dispatch(member, query));
        Throwable failure = cause;
        if (failure != null) {
            return MessageStream.failed(failure);
        }
        MessageStream<QueryResponseMessage> answer = stream;
        return answer == null ? MessageStream.fromIterable(responses) : answer;
    }

    public record Dispatch(Member member, QueryMessage query) {

    }
}
