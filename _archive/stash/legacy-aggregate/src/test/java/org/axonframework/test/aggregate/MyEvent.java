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

package org.axonframework.test.aggregate;

import java.util.Arrays;

/**
 * @author Allard Buijze
 */
public class MyEvent {

    private final Integer someValue;
    private final byte[] someBytes;
    private final Object aggregateIdentifier;

    public MyEvent(Object aggregateIdentifier, Integer someValue) {
        this(aggregateIdentifier, someValue, new byte[]{});
    }

    public MyEvent(Object aggregateIdentifier, Integer someValue, byte[] someBytes) {
        this.aggregateIdentifier = aggregateIdentifier;
        this.someValue = someValue;
        this.someBytes = someBytes;
    }

    public Integer getSomeValue() {
        return someValue;
    }

    public byte[] getSomeBytes() {
        return someBytes;
    }

    public Object getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "someValue=" + someValue +
                ", someBytes=" + Arrays.toString(someBytes) +
                ", aggregateIdentifier=" + aggregateIdentifier +
                '}';
    }
}
