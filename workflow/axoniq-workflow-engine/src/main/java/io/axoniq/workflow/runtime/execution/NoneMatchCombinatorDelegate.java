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

package io.axoniq.workflow.runtime.execution;

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.state.NoneMatchCombinator;
import io.axoniq.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;


/**
 * Default implementation of {@link NoneMatchCombinator}.
 *
 * @author Stefan Dragisic
 * @see NoneMatchCombinator
 * @since 5.4.0
 */
@Internal
public class NoneMatchCombinatorDelegate implements NoneMatchCombinator {

    private final WorkflowExecution workflowExecution;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowExecution the workflow execution.
     */
    @Internal
    public NoneMatchCombinatorDelegate(WorkflowExecution workflowExecution) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow Execution must not be null");
    }

    public CombinatorWorkflowStepResult noneMatch(Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return new CombinatorWorkflowStepResult() {

            private WorkflowStepResult violator;
            private boolean allCompletedNoneMatched;
            private CombinatorSupport.Categories categories;

            private void resolveViolator() {
                if (violator != null || allCompletedNoneMatched) {
                    return;
                }

                var matched = CombinatorSupport.findFirstByPredicate(results, predicate, workflowExecution.state());
                if (matched.isPresent()) {
                    violator = matched.get();
                    return;
                }

                if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                    allCompletedNoneMatched = true;
                    return;
                }

                awaitAndResolve();
            }

            private void awaitAndResolve() {
                try {
                    workflowExecution.awaitStateChange(s ->
                                                               Arrays.stream(results)
                                                                     .filter(WorkflowStepResult::isCompleted)
                                                                     .anyMatch(predicate)
                                                                       || Arrays.stream(results)
                                                                                .allMatch(WorkflowStepResult::isCompleted)
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StepInterruptedException("Interrupted while awaiting noneMatch result", e);
                }

                var matchAfterWait = CombinatorSupport.findFirstByPredicate(results,
                                                                            predicate,
                                                                            workflowExecution.state());
                if (matchAfterWait.isPresent()) {
                    violator = matchAfterWait.get();
                } else {
                    allCompletedNoneMatched = true;
                }
            }

            private CombinatorSupport.Categories categories() {
                if (categories == null) {
                    categories = CombinatorSupport.computeCategories(results, predicate, workflowExecution.state());
                }
                return categories;
            }

            @Override
            public List<WorkflowStepResult> matched() {
                resolveViolator();
                return categories().matched();
            }

            @Override
            public List<WorkflowStepResult> unmatched() {
                resolveViolator();
                return categories().unmatched();
            }

            @Override
            public String getStepName() {
                return "noneMatch(" + String.join(", ",
                                                  Arrays.stream(results).map(WorkflowStepResult::getStepName).toList())
                        + ")";
            }

            @Override
            public boolean isCompleted() {
                return Arrays.stream(results)
                             .filter(WorkflowStepResult::isCompleted)
                             .anyMatch(predicate)
                        || Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted);
            }

            @Override
            public Optional<Map<String, @Nullable Object>> result() {
                resolveViolator();
                if (violator != null) {
                    return violator.result();
                }
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> resultAs(Type type) {
                resolveViolator();
                if (violator != null) {
                    return violator.resultAs(type);
                }
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> resultAs(Type type, Converter converter) {
                resolveViolator();
                if (violator != null) {
                    return violator.resultAs(type, converter);
                }
                return Optional.empty();
            }


            @Override
            public Optional<StepFailedException> error() {
                resolveViolator();
                if (violator != null) {
                    return violator.error();
                }
                return Optional.empty();
            }

            @Override
            public boolean success() {
                resolveViolator();
                return violator == null && allCompletedNoneMatched;
            }

            @Override
            public boolean failure() {
                resolveViolator();
                return violator != null;
            }

            @Override
            public boolean canceled() {
                resolveViolator();
                if (violator != null) {
                    return violator.canceled();
                }
                return false;
            }

            @Override
            public boolean timeout() {
                resolveViolator();
                if (violator != null) {
                    return violator.timeout();
                }
                return false;
            }

            @Override
            public void await() {
                resolveViolator();
            }

            @Override
            public void cancel() {
                Arrays.stream(results).forEach(WorkflowStepResult::cancel);
            }

            @Override
            public void cancel(String reason) {
                Arrays.stream(results).forEach(r -> r.cancel(reason));
            }
        };
    }
}
