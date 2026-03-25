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
package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.core.annotation.Namespace;
import org.axonframework.messaging.queryhandling.annotation.Query;

import static io.axoniq.demo.bikerental.coreapi.rental.FindAvailableQuery.QUERY_NAME;

@Namespace("io.axoniq.demo.bikerental.coreapi.rental")
@Query(name = QUERY_NAME)
public record FindAvailableQuery(
        String bikeType
) {
    public static final String QUERY_NAME = "findAvailable";
}
