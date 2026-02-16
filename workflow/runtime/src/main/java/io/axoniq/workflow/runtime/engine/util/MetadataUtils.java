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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.util;

import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import org.axonframework.messaging.core.Metadata;

import java.util.Optional;
import java.util.function.Predicate;

public class MetadataUtils {

  public static final String METADATA_KEY_WORKFLOW_ID = "workflowId";
  public static final String METADATA_KEY_TYPE = "stepType";
  public static final String METADATA_KEY_STEP_NAME = "stepName";
  public static final String METADATA_KEY_WORKFLOW_STATUS = "workflowStatus";

  public static Metadata create(String workflowId) {
    return Metadata
      .with(METADATA_KEY_WORKFLOW_ID, workflowId);
  }

  public static Metadata create(String workflowId, String stepName, StepStatus stepStatus) {
    return create(workflowId)
      .and(METADATA_KEY_TYPE, stepStatus.name())
      .and(METADATA_KEY_STEP_NAME, stepName);
  }

  public static Metadata create(String workflowId, WorkflowStatus workflowStatus) {
    return create(workflowId)
      .and(METADATA_KEY_WORKFLOW_STATUS, workflowStatus.name());
  }

  public static Optional<WorkflowStatus> getWorkflowStatus(Metadata metadata) {
    if (metadata.containsKey(METADATA_KEY_WORKFLOW_STATUS)) {
      return Optional.of(WorkflowStatus.valueOf(metadata.get(METADATA_KEY_WORKFLOW_STATUS)));
    }
    return Optional.empty();
  }

  public static Optional<StepStatus> getStepStatus(Metadata metadata) {
    if (metadata.containsKey(METADATA_KEY_TYPE)) {
      return Optional.of(StepStatus.valueOf(metadata.get(METADATA_KEY_TYPE)));
    } else {
      return Optional.empty();
    }
  }

  public static String getWorkflowId(Metadata metadata) {
    if (!hasWorkflowId().test(metadata)) {
      throw new IllegalArgumentException("Metadata contains no workflow id");
    }
    return metadata.get(METADATA_KEY_WORKFLOW_ID);
  }

  public static String getStepName(Metadata metadata) {
    return metadata.getOrDefault(METADATA_KEY_STEP_NAME, null);
  }

  public static Predicate<Metadata> workflowIdFilter(String workflowId) {
    return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID) && workflowId.equals(m.get(METADATA_KEY_WORKFLOW_ID));
  }
  public static Predicate<Metadata> hasWorkflowId() {
    return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID);
  }

  private MetadataUtils() {
    // avoid
  }

}
