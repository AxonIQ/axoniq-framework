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

import jakarta.persistence.Entity
import jakarta.persistence.Id

@Entity
class PaymentStatus {
  @Id
  var id: String? = null
    private set

  @JvmField
  var status: Status? = null
  var amount: Int = 0
    private set
  private var reference: String? = null

  constructor()

  constructor(id: String?, amount: Int, reference: String?) {
    this.id = id
    this.amount = amount
    this.reference = reference
    this.status = Status.PENDING
  }

  enum class Status {
    PENDING, APPROVED, REJECTED
  }
}
