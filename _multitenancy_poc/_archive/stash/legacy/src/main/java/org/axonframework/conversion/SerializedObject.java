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

package org.axonframework.conversion;

/**
 * Interface describing the structure of a serialized object.
 *
 * @param <T> The data type representing the serialized object
 * @author Allard Buijze
 * @since 2.0
 * TODO #3602 remove
 * @deprecated By shifting from the {@link Serializer} to the {@link Converter}, this exception becomes obsolete.
 */
@Deprecated(forRemoval = true, since = "5.0.0")
public interface SerializedObject<T> {

    /**
     * Returns the type of this representation's data.
     *
     * @return the type of this representation's data
     */
    Class<T> getContentType();

    /**
     * Returns the description of the type of object contained in the data.
     *
     * @return the description of the type of object contained in the data
     */
    SerializedType getType();

    /**
     * The actual data of the serialized object.
     *
     * @return the actual data of the serialized object
     */
    T getData();

}
