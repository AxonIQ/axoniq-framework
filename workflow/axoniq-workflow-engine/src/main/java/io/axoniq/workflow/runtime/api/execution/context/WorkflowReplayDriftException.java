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
package io.axoniq.workflow.runtime.api.execution.context;


import java.util.List;
import java.util.Objects;

/**
 * Thrown when a workflow's replay reaches an event-emitting primitive that would corrupt an in-flight
 * workflow — specifically, when state contains terminal steps the current invocation has not yet
 * referenced. That signal means the old code ran past this point with steps the new code skips; the
 * new primitive's events would diverge from history.
 * <p>
 * Caught and handled non-terminally by {@code SimpleWorkflowExecution.handleWorkflowException}: a
 * warning is logged and <em>no</em> terminal workflow event is published, so the workflow stays at
 * its current state and the developer can recover by reverting the code change or wrapping the new
 * code in {@code ctx.migrateVersion(changeId, n)} to fork the workflow.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class WorkflowReplayDriftException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String workflowId;
    private final String aboutToExecute;
    private final List<String> orphans;

    /**
     * Constructs a drift exception.
     *
     * @param workflowId     id of the workflow that detected drift.
     * @param aboutToExecute step name about to be executed, or {@code "<terminate>"} for
     *                       workflow-level termination.
     * @param orphans        terminal step names present in history but not referenced by the current
     *                       invocation — the corruption signal.
     */
    public WorkflowReplayDriftException(String workflowId,
                                        String aboutToExecute,
                                        List<String> orphans) {
        super(buildMessage(workflowId, aboutToExecute, orphans));
        this.workflowId = Objects.requireNonNull(workflowId, "workflowId must not be null");
        this.aboutToExecute = Objects.requireNonNull(aboutToExecute, "aboutToExecute must not be null");
        this.orphans = List.copyOf(Objects.requireNonNull(orphans, "orphans must not be null"));
    }

    /**
     * The id of the workflow that could not proceed because of replay drift.
     *
     * @return the workflow id.
     */
    public String workflowId() {
        return workflowId;
    }

    /**
     * The step the current code was about to execute when the drift was detected.
     *
     * @return the name of the step about to execute.
     */
    public String aboutToExecute() {
        return aboutToExecute;
    }

    /**
     * The terminal steps present in history that the current code does not reference — evidence that
     * older code ran past this point.
     *
     * @return the unreferenced (orphan) terminal step names.
     */
    public List<String> orphans() {
        return orphans;
    }

    private static String buildMessage(String workflowId, String aboutToExecute, List<String> orphans) {
        return String.format(
                "Workflow %s cannot proceed at \"%s\" because history contains terminal steps the "
                        + "current code does not reference: %s. This means old code ran past this point "
                        + "with steps the new code skips. Either revert the change or wrap the new code "
                        + "in ctx.migrateVersion(\"<changeId>\", N) so old workflows stay on the legacy branch.",
                workflowId, aboutToExecute, orphans);
    }
}
