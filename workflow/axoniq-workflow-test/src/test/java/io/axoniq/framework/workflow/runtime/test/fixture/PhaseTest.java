/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.fixture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link Phase}.
 *
 * @author Simon Zambrovski
 */
class PhaseTest {

    @org.junit.jupiter.api.Test
    void selfReturnsConcreteStageType() {
        Test stage = new Test();

        assertThat(stage.returnSelf()).isSameAs(stage);
    }

    private static class Test extends Phase<Test> {

        private Test returnSelf() {
            return self();
        }
    }
}
