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
package io.axoniq.demo.bikerental.coreapi.payment

import org.axonframework.eventsourcing.annotation.EventTag
import org.axonframework.messaging.commandhandling.annotation.Command
import org.axonframework.messaging.eventhandling.annotation.Event
import org.axonframework.messaging.queryhandling.annotation.Query
import org.axonframework.modelling.annotation.TargetEntityId

@Command(routingKey = "paymentReference")
@JvmRecord
data class PreparePaymentCommand(
    val amount: Int,
    val paymentReference: String
)

@Command(routingKey = "paymentId")
@JvmRecord
data class ConfirmPaymentCommand(
    @TargetEntityId
    val paymentId: String
)

@Command(routingKey = "paymentId")
@JvmRecord
data class RejectPaymentCommand(
    @TargetEntityId
    val paymentId: String
)

@Event
@JvmRecord
data class PaymentPreparedEvent(
    @EventTag(key = "Payment")
    val paymentId: String,
    val amount: Int,
    val paymentReference: String
)

@Event
@JvmRecord
data class PaymentRejectedEvent(
    @EventTag(key = "Payment")
    val paymentId: String,
    val paymentReference: String
)

@Event
@JvmRecord
data class PaymentConfirmedEvent(
    @EventTag(key = "Payment")
    val paymentId: String,
    val paymentReference: String
)

@Query(namespace = "io.axoniq.demo.bikerental.coreapi.payment", name = "getAllPayments")
@JvmRecord
data class GetAllPaymentsQuery(
    val status: PaymentStatus.Status?
)

@Query(namespace = "io.axoniq.demo.bikerental.coreapi.payment", name = "getPaymentId")
@JvmRecord
data class GetPaymentIdQuery(
    val paymentReference: String
)

@Query(namespace = "io.axoniq.demo.bikerental.coreapi.payment", name = "getPaymentStatus")
@JvmRecord
data class GetPaymentStatusQuery(
    val paymentId: String
)

