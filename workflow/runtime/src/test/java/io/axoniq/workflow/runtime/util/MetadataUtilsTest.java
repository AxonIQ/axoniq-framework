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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

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
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> {
            constructor.newInstance();
        });
    }

    @Test
    void testCreateWithWorkflowId() {
        Metadata metadata = MetadataUtils.create("wf123");
        assertEquals("wf123", metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_ID));
    }

    @Test
    void testCreateWithStepInfo() {
        Metadata metadata = MetadataUtils.create("wf123", "step1", StepStatus.STARTED);
        assertEquals("wf123", metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_ID));
        assertEquals("step1", metadata.get(MetadataUtils.METADATA_KEY_STEP_NAME));
        assertEquals(StepStatus.STARTED.name(), metadata.get(MetadataUtils.METADATA_KEY_TYPE));
    }

    @Test
    void testCreateWithWorkflowStatus() {
        Metadata metadata = MetadataUtils.create("wf123", WorkflowStatus.COMPLETED);
        assertEquals("wf123", metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_ID));
        assertEquals(WorkflowStatus.COMPLETED.name(), metadata.get(MetadataUtils.METADATA_KEY_WORKFLOW_STATUS));
    }

    @Test
    void testGetWorkflowStatus() {
        Metadata metadata = MetadataUtils.create("wf123", WorkflowStatus.STARTED);
        assertEquals(Optional.of(WorkflowStatus.STARTED), MetadataUtils.getWorkflowStatus(metadata));

        Metadata empty = Metadata.emptyInstance();
        assertEquals(Optional.empty(), MetadataUtils.getWorkflowStatus(empty));
    }

    @Test
    void testGetStepStatus() {
        Metadata metadata = MetadataUtils.create("wf123", "step1", StepStatus.COMPLETED);
        assertEquals(Optional.of(StepStatus.COMPLETED), MetadataUtils.getStepStatus(metadata));

        Metadata empty = Metadata.emptyInstance();
        assertEquals(Optional.empty(), MetadataUtils.getStepStatus(empty));
    }

    @Test
    void testPayloadReducer() {
        Metadata metadata = Metadata.with(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, "reducer1");
        assertEquals(Optional.of("reducer1"), MetadataUtils.payloadReducer(metadata));

        Metadata empty = Metadata.emptyInstance();
        assertEquals(Optional.empty(), MetadataUtils.payloadReducer(empty));
    }

    @Test
    void testGetWorkflowId() {
        Metadata metadata = MetadataUtils.create("wf123");
        assertEquals("wf123", MetadataUtils.getWorkflowId(metadata));

        Metadata empty = Metadata.emptyInstance();
        assertThrows(IllegalArgumentException.class, () -> MetadataUtils.getWorkflowId(empty));
    }

    @Test
    void testGetStepName() {
        Metadata metadata = MetadataUtils.create("wf123", "step1", StepStatus.STARTED);
        assertEquals("step1", MetadataUtils.getStepName(metadata));

        Metadata metadataNoStep = MetadataUtils.create("wf123");
        assertNull(MetadataUtils.getStepName(metadataNoStep));
    }

    @Test
    void testWorkflowIdFilter() {
        Metadata metadataMatch = MetadataUtils.create("wf123");
        Metadata metadataMismatch = MetadataUtils.create("wf456");
        Metadata empty = Metadata.emptyInstance();

        var filter = MetadataUtils.workflowIdFilter("wf123");

        assertTrue(filter.test(metadataMatch));
        assertFalse(filter.test(metadataMismatch));
        assertFalse(filter.test(empty));
    }

    @Test
    void testHasWorkflowId() {
        Metadata metadata = MetadataUtils.create("wf123");
        Metadata empty = Metadata.emptyInstance();

        assertTrue(MetadataUtils.hasWorkflowId().test(metadata));
        assertFalse(MetadataUtils.hasWorkflowId().test(empty));
    }
}
