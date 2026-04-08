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

package org.axonframework.common.util;

import java.util.List;

/**
 * Stub Domain Event, used for testing purposes.
 *
 * @author Steven van Beelen
 */
public class ThirdStubEvent {

    private final String name;
    private final Integer number;
    private final List<Boolean> truths;

    // No-arg constructor required for JacksonSerializer
    @SuppressWarnings("unused")
    private ThirdStubEvent() {
        name = null;
        number = null;
        truths = null;
    }

    @SuppressWarnings("unused")
    public ThirdStubEvent(String name, Integer number, List<Boolean> truths) {
        this.name = name;
        this.number = number;
        this.truths = truths;
    }

    public String getName() {
        return name;
    }

    public Integer getNumber() {
        return number;
    }

    public List<Boolean> getTruths() {
        return truths;
    }
}
