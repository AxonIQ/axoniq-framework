/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests the sequencing of the workflow history processor.
 *
 * @author Stefan Dragisic
 */
class WorkflowHistorySequencingPolicyTest {

    private final WorkflowHistorySequencingPolicy policy = new WorkflowHistorySequencingPolicy();

    @Test
    void aWorkflowEventIsSequencedByItsWorkflow() {
        // given
        var event = new GenericEventMessage(new MessageType(new QualifiedName("StepStarted")),
                                            Map.of(),
                                            MetadataUtils.create("order-1", "charge", StepStatus.STARTED));

        // when / then
        assertThat(policy.sequenceIdentifierFor(event, new StubProcessingContext())).contains("order-1");
    }

    @Test
    void anEventPublishedByAWorkflowIsSequencedByItsPublisherSoItsHistoryIsWrittenOnce() {
        // given
        var event = new GenericEventMessage(
                new MessageType(new QualifiedName("OrderConfirmed")),
                Map.of(),
                MetadataUtils.create("order-1", "notify", StepStatus.COMPLETED)
                             .and(MetadataUtils.METADATA_KEY_STEP_PRIMITIVE, MetadataUtils.STEP_PRIMITIVE_PUBLISH)
        );

        // when / then
        assertThat(policy.sequenceIdentifierFor(event, new StubProcessingContext())).contains("order-1");
    }

    @Test
    void aBusinessEventIsSequencedByItsOwnIdentifier() {
        // given
        var event = new GenericEventMessage(new MessageType(new QualifiedName("PaymentReceived")), Map.of());

        // when / then
        assertThat(policy.sequenceIdentifierFor(event, new StubProcessingContext())).contains(event.identifier());
    }
}
