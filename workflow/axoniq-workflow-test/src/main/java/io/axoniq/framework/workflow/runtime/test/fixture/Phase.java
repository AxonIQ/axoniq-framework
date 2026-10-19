/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
