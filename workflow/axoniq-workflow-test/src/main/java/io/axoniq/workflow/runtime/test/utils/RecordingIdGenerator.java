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
package io.axoniq.workflow.runtime.test.utils;

import org.axonframework.common.annotation.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Recording id generator.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class RecordingIdGenerator implements IdGenerator {

    private final List<String> history = new ArrayList<>();

    @Override
    public String next() {
        String id = IdGenerator.super.next();
        history.add(id);
        return id;
    }

    /**
     * Retrieves id history.
     *
     * @return id history
     */
    public List<String> getHistory() {
        return Collections.unmodifiableList(history);
    }

    /**
     * Clears the generation history.
     */
    public void clear() {
        this.history.clear();
    }
}
