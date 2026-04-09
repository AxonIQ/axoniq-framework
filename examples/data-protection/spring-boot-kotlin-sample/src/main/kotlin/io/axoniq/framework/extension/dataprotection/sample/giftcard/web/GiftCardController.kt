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
package io.axoniq.framework.extension.dataprotection.sample.giftcard.web

import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.IssueGiftCardCommand
import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.RedeemGiftCardCommand
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Address
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.FindAllGiftCardsQuery
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.FindGiftCardQuery
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.GiftCardSummary
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.GiftCardSummaryList
import org.axonframework.messaging.commandhandling.gateway.CommandGateway
import org.axonframework.messaging.queryhandling.gateway.QueryGateway
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture

/**
 * REST controller providing HTTP endpoints for gift card operations.
 *
 * This controller serves as the primary HTTP interface for the gift card management system,
 * implementing RESTful endpoints that bridge web clients with the CQRS/Event Sourcing backend.
 * It leverages Spring WebFlux for reactive programming and provides both traditional REST
 * endpoints and real-time Server-Sent Events (SSE) streams.
 *
 * Architecture integration:
 * - Command Side: Uses CommandGateway for dispatching commands
 * - Query Side: Uses QueryGateway for both synchronous and subscription queries
 * - Reactive Streams: Leverages Spring WebFlux and Project Reactor for SSE
 * - CORS Support: Enables cross-origin requests for web client integration
 *
 * Endpoint categories:
 * - Command endpoints: POST operations for issuing and redeeming gift cards
 * - Query endpoints: GET operations for retrieving gift card data
 * - Streaming endpoints: SSE endpoints for real-time updates
 *
 * Real-time capabilities:
 * The controller provides Server-Sent Events endpoints that enable real-time updates
 * to connected web clients. This allows for live dashboards and immediate UI updates
 * without polling, enhancing user experience and reducing server load.
 *
 * Error handling:
 * The controller implements proper error handling patterns with CompletableFuture
 * exception handling, returning appropriate HTTP status codes and error messages
 * for various failure scenarios (validation errors, insufficient funds, etc.).
 *
 * Request/Response patterns:
 * - Commands return HTTP 200 with operation results or identifiers
 * - Queries return HTTP 200 with data or HTTP 404 for missing resources
 * - Streaming endpoints return continuous data streams with proper content types
 * - Error scenarios return appropriate 4xx/5xx status codes
 *
 */
@RestController
@RequestMapping("/api/giftcards")
@CrossOrigin(origins = ["*"])
class GiftCardController(
    private val commandGateway: CommandGateway,
    private val queryGateway: QueryGateway
) {

    /**
     * Issues a new gift card with the specified amount.
     *
     * This endpoint creates a new gift card by generating a unique identifier and
     * dispatching an IssueGiftCardCommand through the command gateway. The
     * operation is asynchronous and returns the generated gift card ID upon successful
     * completion.
     *
     * HTTP mapping: POST /api/giftcards
     *
     * Request body: JSON containing the initial amount
     * ```
     * {
     *   "amount": 50.00
     * }
     * ```
     *
     * Success response: HTTP 200 with the new gift card ID
     * Error cases: HTTP 400 for validation errors (negative amounts, etc.)
     *
     * @param request the issue request containing the initial gift card amount
     * @return CompletableFuture containing ResponseEntity with the generated gift card ID
     */
    @PostMapping
    fun issueGiftCard(@RequestBody request: IssueRequest): CompletableFuture<ResponseEntity<String>> {
        val giftCardId = UUID.randomUUID().toString()

        // Create Person object from request fields
        val owner = Person(
            name = request.ownerName,
            address = Address(
                line1 = request.addressLine1,
                line2 = request.addressLine2,
                country = request.country
            )
        )

        return commandGateway.send(
            IssueGiftCardCommand(
                giftCardId = giftCardId,
                amount = request.amount,
                username = request.username,
                owner = owner,
                dateOfBirth = request.dateOfBirth,
                randomNumber = request.randomNumber
            )
        )
            .resultMessage
            .thenApply { ResponseEntity.ok(giftCardId) }
    }

    /**
     * Redeems a specified amount from an existing gift card.
     *
     * This endpoint processes a redemption by dispatching a RedeemGiftCardCommand
     * for the specified gift card. The operation validates that sufficient funds are available
     * and updates the gift card balance accordingly.
     *
     * HTTP mapping: POST /api/giftcards/{giftCardId}/redeem
     *
     * Path variable: giftCardId - the unique identifier of the gift card
     * Request body: JSON containing the redemption amount
     * ```
     * {
     *   "amount": 25.00
     * }
     * ```
     *
     * Success response: HTTP 200 with success message
     * Error cases:
     * - HTTP 400 for insufficient funds or invalid amounts
     * - HTTP 400 for non-existent gift card IDs
     *
     * @param giftCardId the unique identifier of the gift card to redeem from
     * @param request the redeem request containing the amount to redeem
     * @return CompletableFuture containing ResponseEntity with success message or error details
     */
    @PostMapping("/{giftCardId}/redeem")
    fun redeemGiftCard(
        @PathVariable("giftCardId") giftCardId: String,
        @RequestBody request: RedeemRequest
    ): CompletableFuture<ResponseEntity<String>> {
        return commandGateway.send(RedeemGiftCardCommand(giftCardId, request.amount))
            .resultMessage
            .thenApply { ResponseEntity.ok("Redeemed successfully") }
            .exceptionally { throwable -> ResponseEntity.badRequest().body(throwable.message) }
    }

    /**
     * Retrieves all gift cards in the system.
     *
     * This endpoint queries the read model to return a list of all gift cards with
     * their current states. The response includes gift card IDs, remaining balances,
     * and initial values for comprehensive dashboard displays.
     *
     * HTTP mapping: GET /api/giftcards
     *
     * Response: JSON array of gift card summary objects
     * ```
     * [
     *   {
     *     "giftCardId": "uuid-123",
     *     "remainingValue": 25.00,
     *     "initialValue": 50.00
     *   }
     * ]
     * ```
     *
     * Performance note: This endpoint returns all gift cards without
     * pagination. For production systems with large datasets, consider implementing
     * pagination parameters.
     *
     * @return CompletableFuture containing a list of all gift card summaries
     */
    @GetMapping
    fun getAllGiftCards(): CompletableFuture<List<GiftCardSummary>> {
        return queryGateway.query(FindAllGiftCardsQuery(), GiftCardSummaryList::class.java)
            .thenApply { it.giftCards }
    }

    /**
     * Retrieves a specific gift card by its unique identifier.
     *
     * This endpoint queries the read model for a single gift card, returning its
     * current state including remaining balance and original value. Returns HTTP 404
     * if the gift card is not found.
     *
     * HTTP mapping: GET /api/giftcards/{giftCardId}
     *
     * Path variable: giftCardId - the unique identifier of the gift card
     *
     * Success response: HTTP 200 with gift card summary JSON
     * ```
     * {
     *   "giftCardId": "uuid-123",
     *   "remainingValue": 25.00,
     *   "initialValue": 50.00
     * }
     * ```
     *
     * Not found response: HTTP 404 when gift card doesn't exist
     *
     * @param giftCardId the unique identifier of the gift card to retrieve
     * @return CompletableFuture containing ResponseEntity with gift card summary or 404 status
     */
    @GetMapping("/{giftCardId}")
    fun getGiftCard(@PathVariable("giftCardId") giftCardId: String): CompletableFuture<ResponseEntity<GiftCardSummary>> {
        return queryGateway.query(FindGiftCardQuery(giftCardId), GiftCardSummary::class.java)
            .thenApply { giftCard ->
                if (giftCard != null) {
                    ResponseEntity.ok(giftCard)
                } else {
                    ResponseEntity.notFound().build()
                }
            }
    }

    /**
     * Provides real-time updates for a specific gift card via Server-Sent Events (SSE).
     *
     * This endpoint establishes a persistent connection that streams gift card updates
     * in real-time. Clients receive the initial gift card state immediately, followed by
     * live updates whenever the gift card is modified (issued or redeemed).
     *
     * HTTP mapping: GET /api/giftcards/{giftCardId}/updates
     * Content-Type: text/event-stream
     * Path variable: giftCardId - the unique identifier of the gift card
     *
     * SSE stream format:
     * ```
     * data: {"giftCardId":"uuid-123","remainingValue":25.00,"initialValue":50.00}
     *
     * data: {"giftCardId":"uuid-123","remainingValue":15.00,"initialValue":50.00}
     * ```
     *
     * Use cases:
     * - Real-time gift card balance displays
     * - Live transaction monitoring
     * - Immediate UI updates without polling
     *
     * Connection management: The subscription is automatically closed
     * when the client disconnects, preventing resource leaks.
     *
     * @param giftCardId the unique identifier of the gift card to monitor
     * @return Flux stream of gift card summary updates
     */
    @GetMapping("/{giftCardId}/updates", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun getGiftCardUpdates(@PathVariable("giftCardId") giftCardId: String): Flux<GiftCardSummary> {
        return Flux.from(queryGateway.subscriptionQuery(FindGiftCardQuery(giftCardId), GiftCardSummary::class.java))
    }

    /**
     * Request DTO for gift card issuance operations.
     *
     * This record encapsulates the data required to issue a new gift card,
     * including the initial amount and all personal data fields.
     *
     * @param amount the initial amount to load onto the new gift card (must be positive)
     * @param username encrypted username
     * @param ownerName deep encrypted owner name
     * @param addressLine1 deep encrypted address line 1
     * @param addressLine2 deep encrypted address line 2
     * @param country not encrypted country
     * @param dateOfBirth serialized encrypted date of birth
     * @param randomNumber serialized encrypted random number
     */
    data class IssueRequest(
        val amount: BigDecimal,
        val username: String,
        val ownerName: String,
        val addressLine1: String,
        val addressLine2: String,
        val country: String,
        val dateOfBirth: LocalDate,
        val randomNumber: Int
    )

    /**
     * Request DTO for gift card redemption operations.
     *
     * This record encapsulates the data required to redeem from an existing
     * gift card. The gift card ID is provided as a path parameter in the endpoint.
     *
     * @param amount the amount to redeem from the gift card (must be positive and not exceed balance)
     */
    data class RedeemRequest(val amount: BigDecimal)
}
