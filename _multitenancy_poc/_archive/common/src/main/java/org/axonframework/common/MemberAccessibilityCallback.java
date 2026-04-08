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

package org.axonframework.common;

import java.lang.reflect.AccessibleObject;
import java.security.PrivilegedAction;

/**
 * PrivilegedAction that makes the given method accessible for reflection.
 *
 * @author Allard Buijze
 * @since 0.5
 */
public class MemberAccessibilityCallback implements PrivilegedAction<Object> {

    private final AccessibleObject method;

    /**
     * Initialize the callback to make the given {@code method} accessible for reflection.
     *
     * @param method The method to make accessible
     */
    public MemberAccessibilityCallback(AccessibleObject method) {
        this.method = method;
    }

    @Override
    public Object run() {
        method.setAccessible(true);
        return null;
    }
}
