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
package io.axoniq.demo.bikerental.payment;

import io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentStatusRepository extends CrudRepository<PaymentStatus, String> {

    List<PaymentStatus> findAllByStatus(PaymentStatus.Status status);

    Optional<PaymentStatus> findByReferenceAndStatus(String reference, PaymentStatus.Status status);
}
