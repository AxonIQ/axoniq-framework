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

package org.axonframework.config;

import java.lang.annotation.*;

/**
 * Hint for the Configuration API that the annotated Event Handler object should be assigned to an Event Processor with
 * the specified name.
 * <p>
 * Explicitly provided assignment rules set in the configuration will overrule hints provided by this annotation.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@Deprecated(since = "5.0.0", forRemoval = true)
public @interface ProcessingGroup {

    /**
     * The name of the Event Processor to assign the annotated Event Handler object to.
     *
     * @return the name of the Event Processor to assign objects of this type to
     */
    String value();
}
