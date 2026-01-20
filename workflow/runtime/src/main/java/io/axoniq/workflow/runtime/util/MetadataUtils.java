package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.engine.StepStatus;
import org.axonframework.messaging.core.Metadata;

import java.util.Optional;
import java.util.function.Predicate;

public class MetadataUtils {

  public static final String METADATA_KEY_WORKFLOW_ID = "workflowId";
  public static final String METADATA_KEY_TYPE = "stepType";
  public static final String METADATA_KEY_STEP_NAME = "stepName";

  public static Metadata create(String workflowId) {
    return Metadata
      .with(METADATA_KEY_WORKFLOW_ID, workflowId);
  }

  public static Metadata create(String workflowId, String stepName, StepStatus stepStatus) {
    return create(workflowId)
      .and(METADATA_KEY_TYPE, stepStatus.name())
      .and(METADATA_KEY_STEP_NAME, stepName);
  }

  public static Optional<StepStatus> getStepStatus(Metadata metadata) {
    if (metadata.containsKey(METADATA_KEY_TYPE)) {
      return Optional.of(StepStatus.valueOf(metadata.get(METADATA_KEY_TYPE)));
    } else {
      return Optional.empty();
    }
  }

  public static String getStepName(Metadata metadata) {
    return metadata.getOrDefault(METADATA_KEY_STEP_NAME, null);
  }

  public static Predicate<Metadata> workflowIdFilter(String workflowId) {
    return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID) && workflowId.equals(m.get(METADATA_KEY_WORKFLOW_ID));
  }

  private MetadataUtils() {
    // avoid
  }

}
