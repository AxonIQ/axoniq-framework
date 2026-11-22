/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.inmemory;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndexContractTest;

/**
 * Tests for {@link InMemoryWorkflowIndex}.
 *
 * @author Allard Buijze
 */
class InMemoryWorkflowIndexTest extends WorkflowIndexContractTest {

    @Override
    protected WorkflowIndex createIndex() {
        return new InMemoryWorkflowIndex();
    }
}
