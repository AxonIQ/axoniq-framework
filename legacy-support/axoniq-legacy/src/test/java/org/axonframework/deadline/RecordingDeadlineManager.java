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

package org.axonframework.deadline;

import org.axonframework.messaging.ScopeDescriptor;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A test-only {@link AbstractDeadlineManager} recording every call instead of scheduling against a backend. Each method
 * follows the Axon Framework 4 {@code SimpleDeadlineManager}: whatever the caller needs back is computed right away,
 * everything else runs through {@code runOnPrepareCommitOrNow(...)}. A scheduled deadline passes the registered
 * dispatch interceptors within that call, with the context the call runs for, as it would before reaching a backend.
 * The cancel calls use the Axon Framework 4 {@link Runnable} variant, so that both variants are exercised.
 */
final class RecordingDeadlineManager extends AbstractDeadlineManager {

    final List<ScheduledCall> scheduled = new CopyOnWriteArrayList<>();
    private final String name;
    private final List<String> timeline;

    /**
     * Creates a manager that records its calls under the name {@code "manager"} in a timeline of its own.
     */
    RecordingDeadlineManager() {
        this("manager", new CopyOnWriteArrayList<>());
    }

    /**
     * Creates a manager that records its calls, prefixed with the given {@code name}, in the given {@code timeline}.
     *
     * @param name     the prefix of every timeline entry this manager adds
     * @param timeline the timeline to add an entry to for every call that runs
     */
    RecordingDeadlineManager(String name, List<String> timeline) {
        this.name = name;
        this.timeline = timeline;
    }

    @Override
    public String schedule(Instant triggerDateTime,
                           String deadlineName,
                           @Nullable Object messageOrPayload,
                           ScopeDescriptor deadlineScope) {
        DeadlineMessage deadlineMessage = asDeadlineMessage(deadlineName, messageOrPayload, triggerDateTime);
        String scheduleId = deadlineMessage.identifier();
        runOnPrepareCommitOrNow(context -> {
            DeadlineMessage intercepted = processDispatchInterceptors(deadlineMessage, context);
            scheduled.add(new ScheduledCall(intercepted, deadlineScope));
            timeline.add(name + ":schedule " + deadlineName);
        });
        return scheduleId;
    }

    @Override
    public void cancelSchedule(String deadlineName, String scheduleId) {
        runOnPrepareCommitOrNow(context -> timeline.add(name + ":cancelSchedule " + deadlineName + "/" + scheduleId));
    }

    @Override
    public void cancelAll(String deadlineName) {
        runOnPrepareCommitOrNow(context -> timeline.add(name + ":cancelAll " + deadlineName));
    }

    @Override
    public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        runOnPrepareCommitOrNow(context -> timeline.add(
                name + ":cancelAllWithinScope " + deadlineName + "@" + scope.scopeDescription()
        ));
    }

    /**
     * A deadline that was scheduled, as it left the dispatch interceptors.
     *
     * @param message the scheduled deadline message
     * @param scope   the scope the deadline was scheduled within
     */
    record ScheduledCall(DeadlineMessage message, ScopeDescriptor scope) {

    }
}
