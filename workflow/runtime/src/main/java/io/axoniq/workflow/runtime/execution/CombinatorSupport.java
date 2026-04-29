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

import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import org.axonframework.common.annotation.Internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Package-private utility that extracts the shared categorization and ordering logic
 * used by all three combinator delegates ({@code AnyMatch}, {@code NoneMatch}, {@code AllMatch}).
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
final class CombinatorSupport {

    private CombinatorSupport() {
    }

    /**
     * Categorized matched/unmatched result pair.
     */
    record Categories(List<WorkflowStepResult> matched, List<WorkflowStepResult> unmatched) {
    }

    /**
     * Splits the results array into matched (completed + satisfies predicate) and unmatched (everything else).
     * Matched results are sorted by event-sourced timestamp.
     * Unmatched results list completed (sorted by timestamp) first, then not-yet-completed in array order.
     */
    static Categories computeCategories(WorkflowStepResult[] results,
                                        Predicate<WorkflowStepResult> predicate,
                                        WorkflowState workflowState) {
        var matchedResults = Arrays.stream(results)
                                   .filter(WorkflowStepResult::isCompleted)
                                   .filter(predicate)
                                   .toList();
        var matchedNames = matchedResults.stream()
                                         .map(WorkflowStepResult::getStepName)
                                         .collect(Collectors.toSet());
        var unmatchedResults = Arrays.stream(results)
                                     .filter(r -> !matchedNames.contains(r.getStepName()))
                                     .toList();
        return new Categories(
                sortByEventSourcedTimestamp(matchedResults, workflowState),
                sortMixed(unmatchedResults, workflowState)
        );
    }

    /**
     * Finds the first completed result matching the given predicate, ordered by event-sourced timestamp.
     * Falls back to stream order when event-sourced ordering is unavailable.
     */
    static Optional<WorkflowStepResult> findFirstByPredicate(WorkflowStepResult[] results,
                                                             Predicate<WorkflowStepResult> matchPredicate,
                                                             WorkflowState workflowState) {
        var matchedNames = Arrays.stream(results)
                                 .filter(WorkflowStepResult::isCompleted) //todo, should we remove this predicate? so people can do notCompleted predicate?
                                 .filter(matchPredicate)
                                 .map(WorkflowStepResult::getStepName)
                                 .collect(Collectors.toSet());
        if (matchedNames.isEmpty()) {
            return Optional.empty();
        }
        return workflowState.firstCompletedAmong(matchedNames)
                             .flatMap(name -> Arrays.stream(results)
                                                    .filter(r -> r.getStepName().equals(name))
                                                    .findFirst())
                             .or(() -> Arrays.stream(results)
                                             .filter(WorkflowStepResult::isCompleted)
                                             .filter(matchPredicate)
                                             .findFirst());
    }

    /**
     * Sorts completed results by event-sourced timestamp. Falls back to the input order
     * when the workflow state cannot provide a timestamp ordering.
     */
    static List<WorkflowStepResult> sortByEventSourcedTimestamp(List<WorkflowStepResult> items,
                                                                WorkflowState workflowState) {
        if (items.isEmpty()) {
            return List.of();
        }
        var names = items.stream()
                         .map(WorkflowStepResult::getStepName)
                         .collect(Collectors.toSet());
        var sortedNames = workflowState.sortedCompletedAmong(names);
        if (!sortedNames.isEmpty()) {
            return sortedNames.stream()
                              .flatMap(name -> items.stream()
                                                    .filter(r -> r.getStepName().equals(name)))
                              .toList();
        }
        return Collections.unmodifiableList(items);
    }

    /**
     * Sorts a mixed list: completed results by event-sourced timestamp first,
     * then not-yet-completed results in their original order.
     */
    private static List<WorkflowStepResult> sortMixed(List<WorkflowStepResult> items,
                                                      WorkflowState workflowState) {
        var completed = items.stream().filter(WorkflowStepResult::isCompleted).toList();
        var notCompleted = items.stream().filter(r -> !r.isCompleted()).toList();
        var sorted = new ArrayList<>(sortByEventSourcedTimestamp(completed, workflowState));
        sorted.addAll(notCompleted);
        return Collections.unmodifiableList(sorted);
    }
}
