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

package org.axonframework.modelling.annotation;

import org.axonframework.modelling.PropertyBasedEntityIdResolver;

/**
 * Exception indicating that while using the {@link PropertyBasedEntityIdResolver} the indicated field could not be
 * found.
 * <p>
 * This indicates a mismatch between the property configured in the {@link InjectEntity#idProperty()} and the actual
 * payload class.
 *
 * @author Mitchell Herrijgers
 * @see PropertyBasedEntityIdResolver
 * @see InjectEntity
 * @since 5.0.0
 */
public class TargetEntityIdMemberMismatchException extends RuntimeException {

    /**
     * Initialize the exception with the given {@code fieldName} that was not found in the payload of type
     * {@code payloadClass}.
     */
    public TargetEntityIdMemberMismatchException(String fieldName, Class<?> payloadClass) {
        super(String.format(
                "Could not find field [%s] or its accessor in payload of type [%s] as indicated on the @InjectModel annotation.",
                fieldName,
                payloadClass.getName()));
    }
}
