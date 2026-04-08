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


/**
 * PropertyAccessStrategy implementation that finds properties defined according to the Uniform Access Principle
 * (see <a href="http://en.wikipedia.org/wiki/Uniform_access_principle">Wikipedia</a>).
 * For example, a property called {@code myProperty}, it will use a method called {@code myProperty()};
 *
 * @author Maxim Fedorov
 * @author Allard Buijze
 * @since 2.0
 */
public class UniformPropertyAccessStrategy extends AbstractMethodPropertyAccessStrategy {

    @Override
    protected String getterName(String property) {
        return property;
    }

    @Override
    protected int getPriority() {
        return -1024;
    }
}
