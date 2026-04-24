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

package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link MetadataUtils}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class MetadataUtilsTest {

    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<MetadataUtils> constructor = MetadataUtils.class.getDeclaredConstructor();
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();
        constructor.setAccessible(true);
        assertThatCode(constructor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void testCreateWithWorkflowId() {
        Metadata metadata = MetadataUtils.create("wf123");
        assertThat(metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_ID)).isEqualTo("wf123");
    }

    @Test
    void testCreateWithStepInfo() {
        Metadata metadata = MetadataUtils.create("wf123", "step1", StepStatus.STARTED);
        assertThat(metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_ID)).isEqualTo("wf123");
        assertThat(metadata.get(MetadataUtils.METADATA_KEY_STEP_NAME)).isEqualTo("step1");
        assertThat(metadata.get(MetadataUtils.METADATA_KEY_TYPE)).isEqualTo(StepStatus.STARTED.name());
    }

    @Test
    void testCreateWithWorkflowStatus() {
        Metadata metadata = MetadataUtils.create("wf123", WorkflowStatus.COMPLETED);
        assertThat(metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_ID)).isEqualTo("wf123");
        assertThat(metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_STATUS)).isEqualTo(WorkflowStatus.COMPLETED.name());
    }

    @Test
    void testGetWorkflowStatus() {
        Metadata metadata = MetadataUtils.create("wf123", WorkflowStatus.STARTED);
        assertThat(MetadataUtils.getWorkflowStatus(metadata)).contains(WorkflowStatus.STARTED);

        Metadata empty = Metadata.emptyInstance();
        assertThat(MetadataUtils.getWorkflowStatus(empty)).isEmpty();
    }

    @Test
    void testGetStepStatus() {
        Metadata metadata = MetadataUtils.create("wf123", "step1", StepStatus.COMPLETED);
        assertThat(MetadataUtils.getStepStatus(metadata)).contains(StepStatus.COMPLETED);

        Metadata empty = Metadata.emptyInstance();
        assertThat(MetadataUtils.getStepStatus(empty)).isEmpty();
    }

    @Test
    void testPayloadReducer() {
        Metadata metadata = Metadata.with(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, "reducer1");
        assertThat(MetadataUtils.payloadReducer(metadata)).contains("reducer1");

        Metadata empty = Metadata.emptyInstance();
        assertThat(MetadataUtils.payloadReducer(empty)).isEmpty();
    }

    @Test
    void testGetWorkflowId() {
        Metadata metadata = MetadataUtils.create("wf123");
        assertThat(MetadataUtils.getWorkflowId(metadata)).isEqualTo("wf123");

        Metadata empty = Metadata.emptyInstance();
        assertThatThrownBy(() -> MetadataUtils.getWorkflowId(empty)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetStepName() {
        Metadata metadata = MetadataUtils.create("wf123", "step1", StepStatus.STARTED);
        assertThat(MetadataUtils.getStepName(metadata)).isEqualTo("step1");

        Metadata metadataNoStep = MetadataUtils.create("wf123");
        assertThat(MetadataUtils.getStepName(metadataNoStep)).isNull();
    }

    @Test
    void testWorkflowIdFilter() {
        Metadata metadataMatch = MetadataUtils.create("wf123");
        Metadata metadataMismatch = MetadataUtils.create("wf456");
        Metadata empty = Metadata.emptyInstance();

        var filter = MetadataUtils.workflowIdFilter("wf123");

        assertThat(filter.test(metadataMatch)).isTrue();
        assertThat(filter.test(metadataMismatch)).isFalse();
        assertThat(filter.test(empty)).isFalse();
    }

    @Test
    void testHasWorkflowId() {
        Metadata metadata = MetadataUtils.create("wf123");
        Metadata empty = Metadata.emptyInstance();

        assertThat(MetadataUtils.hasWorkflowId().test(metadata)).isTrue();
        assertThat(MetadataUtils.hasWorkflowId().test(empty)).isFalse();
    }
}
