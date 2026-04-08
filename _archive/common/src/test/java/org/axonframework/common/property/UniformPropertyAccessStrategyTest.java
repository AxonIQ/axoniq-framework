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

package org.axonframework.common.property;


import static org.junit.jupiter.api.Assertions.fail;

public class UniformPropertyAccessStrategyTest
        extends AbstractPropertyAccessStrategyTest<UniformPropertyAccessStrategyTest.TestMessage> {

    @Override
    protected String exceptionPropertyName() {
        return "exceptionProperty";
    }

    @Override
    protected String regularPropertyName() {
        return "actualProperty";
    }

    @Override
    protected String unknownPropertyName() {
        return "bogusProperty";
    }

    @Override
    protected TestMessage propertyHoldingInstance() {
        return new TestMessage();
    }

    @Override
    protected Property<TestMessage> getProperty(String property) {
        return new UniformPropertyAccessStrategy().propertyFor(TestMessage.class, property);
    }

    @Override
    protected String voidPropertyName() {
        return "voidMethod";
    }

    @SuppressWarnings("UnusedDeclaration")
    static class TestMessage {

        public String actualProperty() {
            return "value";
        }

        public String exceptionProperty() {
            throw new RuntimeException("GetTestException");
        }

        public void voidMethod() {
            fail("This method should never be invoked");
        }
    }
}
