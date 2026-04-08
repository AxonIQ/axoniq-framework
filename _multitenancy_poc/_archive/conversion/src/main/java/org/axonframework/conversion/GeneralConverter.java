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
 * A general-purpose {@link Converter} that places no restrictions on the types it can convert.
 * <p>
 * Serves as a dedicated contract for components that perform unrestricted object conversion, in contrast to
 * type-specific converters like {@code MessageConverter} or {@code EventConverter} which restrict conversion to
 * particular message types. Use this interface when conversion is not tied to a specific message abstraction.
 * <p>
 * Implementations of this interface typically delegate to a {@link Converter}, such as
 * {@link DelegatingGeneralConverter}.
 *
 * @author Jakob Hatzl
 * @since 5.1.0
 */
public interface GeneralConverter extends Converter {

}
