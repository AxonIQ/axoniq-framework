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
package io.axoniq.framework.workflow.runtime.test.fixture;

import org.axonframework.common.annotation.Internal;

/**
 * Base phases for BDD phase.
 *
 * @param <SELF> type of the phase
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class Phase<SELF extends Phase<SELF>> {

    @Internal
    @SuppressWarnings("unchecked")
    protected <T extends SELF> T self() {
        return (T) this;
    }
}
