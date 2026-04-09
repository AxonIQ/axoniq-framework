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
package io.axoniq.framework.extension.dataprotection.sample.giftcard.web;

import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.IssueGiftCardCommand;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.command.RedeemGiftCardCommand;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Address;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.Person;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.FindAllGiftCardsQuery;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.FindGiftCardQuery;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.GiftCardSummary;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.query.GiftCardSummaryList;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller providing HTTP endpoints for gift card operations.
 *
 * <p>This controller serves as the primary HTTP interface for the gift card management system,
 * implementing RESTful endpoints that bridge web clients with the CQRS/Event Sourcing backend.
 * It leverages Spring WebFlux for reactive programming and provides both traditional REST
 * endpoints and real-time Server-Sent Events (SSE) streams.</p>
 *
 * <p><strong>Architecture integration:</strong></p>
 * <ul>
 *   <li><strong>Command Side</strong>: Uses {@link CommandGateway} for dispatching commands</li>
 *   <li><strong>Query Side</strong>: Uses {@link QueryGateway} for both synchronous and subscription queries</li>
 *   <li><strong>Reactive Streams</strong>: Leverages Spring WebFlux and Project Reactor for SSE</li>
 *   <li><strong>CORS Support</strong>: Enables cross-origin requests for web client integration</li>
 * </ul>
 *
 * <p><strong>Endpoint categories:</strong></p>
 * <ul>
 *   <li><strong>Command endpoints</strong>: POST operations for issuing and redeeming gift cards</li>
 *   <li><strong>Query endpoints</strong>: GET operations for retrieving gift card data</li>
 *   <li><strong>Streaming endpoints</strong>: SSE endpoints for real-time updates</li>
 * </ul>
 *
 * <p><strong>Real-time capabilities:</strong></p>
 * <p>The controller provides Server-Sent Events endpoints that enable real-time updates
 * to connected web clients. This allows for live dashboards and immediate UI updates
 * without polling, enhancing user experience and reducing server load.</p>
 *
 * <p><strong>Error handling:</strong></p>
 * <p>The controller implements proper error handling patterns with CompletableFuture
 * exception handling, returning appropriate HTTP status codes and error messages
 * for various failure scenarios (validation errors, insufficient funds, etc.).</p>
 *
 * <p><strong>Request/Response patterns:</strong></p>
 * <ul>
 *   <li>Commands return HTTP 200 with operation results or identifiers</li>
 *   <li>Queries return HTTP 200 with data or HTTP 404 for missing resources</li>
 *   <li>Streaming endpoints return continuous data streams with proper content types</li>
 *   <li>Error scenarios return appropriate 4xx/5xx status codes</li>
 * </ul>
 *
 * @version 1.0
 * @see CommandGateway
 * @see QueryGateway
 * @see <a href="https://docs.axoniq.io/reference-guide/">Axon Framework Reference Guide</a>
 * @since 1.0
 */
@RestController
@RequestMapping("/api/giftcards")
@CrossOrigin(origins = "*")
public class GiftCardController {

    /**
     * Axon Framework command gateway for dispatching commands to EventSourced DCB models.
     * Handles command validation, routing, and result handling.
     */
    private final CommandGateway commandGateway;

    /**
     * Axon Framework query gateway for executing queries and subscription queries.
     * Supports both synchronous queries and real-time subscription queries.
     */
    private final QueryGateway queryGateway;

    /**
     * Constructs a new GiftCardController with required Axon Framework gateways.
     *
     * @param commandGateway the command gateway for dispatching commands
     * @param queryGateway   the query gateway for executing queries and subscriptions
     */
    public GiftCardController(CommandGateway commandGateway, QueryGateway queryGateway) {
        this.commandGateway = commandGateway;
        this.queryGateway = queryGateway;
    }

    /**
     * Issues a new gift card with the specified amount.
     *
     * <p>This endpoint creates a new gift card by generating a unique identifier and
     * dispatching an {@link IssueGiftCardCommand} through the command gateway. The
     * operation is asynchronous and returns the generated gift card ID upon successful
     * completion.</p>
     *
     * <p><strong>HTTP mapping:</strong> {@code POST /api/giftcards}</p>
     *
     * <p><strong>Request body:</strong> JSON containing the initial amount</p>
     * <pre>{@code
     * {
     *   "amount": 50.00
     * }
     * }</pre>
     *
     * <p><strong>Success response:</strong> HTTP 200 with the new gift card ID</p>
     * <p><strong>Error cases:</strong> HTTP 400 for validation errors (negative amounts, etc.)</p>
     *
     * @param request the issue request containing the initial gift card amount
     * @return CompletableFuture containing ResponseEntity with the generated gift card ID
     */
    @PostMapping
    public CompletableFuture<ResponseEntity<String>> issueGiftCard(@RequestBody IssueRequest request) {
        String giftCardId = UUID.randomUUID().toString();

        // Create Person object from request fields
        Person owner = new Person(
                request.ownerName(),
                new Address(request.addressLine1(), request.addressLine2(), request.country())
        );

        return commandGateway.send(new IssueGiftCardCommand(
                giftCardId,
                request.amount(),
                request.username(),
                owner,
                request.dateOfBirth(),
                request.randomNumber()
        ))
                .getResultMessage()
                .thenApply(result -> ResponseEntity.ok(giftCardId));
    }

    /**
     * Redeems a specified amount from an existing gift card.
     *
     * <p>This endpoint processes a redemption by dispatching a {@link RedeemGiftCardCommand}
     * for the specified gift card. The operation validates that sufficient funds are available
     * and updates the gift card balance accordingly.</p>
     *
     * <p><strong>HTTP mapping:</strong> {@code POST /api/giftcards/{giftCardId}/redeem}</p>
     *
     * <p><strong>Path variable:</strong> {@code giftCardId} - the unique identifier of the gift card</p>
     * <p><strong>Request body:</strong> JSON containing the redemption amount</p>
     * <pre>{@code
     * {
     *   "amount": 25.00
     * }
     * }</pre>
     *
     * <p><strong>Success response:</strong> HTTP 200 with success message</p>
     * <p><strong>Error cases:</strong></p>
     * <ul>
     *   <li>HTTP 400 for insufficient funds or invalid amounts</li>
     *   <li>HTTP 400 for non-existent gift card IDs</li>
     * </ul>
     *
     * @param giftCardId the unique identifier of the gift card to redeem from
     * @param request    the redeem request containing the amount to redeem
     * @return CompletableFuture containing ResponseEntity with success message or error details
     */
    @PostMapping("/{giftCardId}/redeem")
    public CompletableFuture<ResponseEntity<String>> redeemGiftCard(
            @PathVariable("giftCardId") String giftCardId,
            @RequestBody RedeemRequest request) {
        return commandGateway.send(new RedeemGiftCardCommand(giftCardId, request.amount()))
                .getResultMessage()
                .thenApply(result -> ResponseEntity.ok("Redeemed successfully"))
                .exceptionally(throwable -> ResponseEntity.badRequest().body(throwable.getMessage()));
    }

    /**
     * Retrieves all gift cards in the system.
     *
     * <p>This endpoint queries the read model to return a list of all gift cards with
     * their current states. The response includes gift card IDs, remaining balances,
     * and initial values for comprehensive dashboard displays.</p>
     *
     * <p><strong>HTTP mapping:</strong> {@code GET /api/giftcards}</p>
     *
     * <p><strong>Response:</strong> JSON array of gift card summary objects</p>
     * <pre>{@code
     * [
     *   {
     *     "giftCardId": "uuid-123",
     *     "remainingValue": 25.00,
     *     "initialValue": 50.00
     *   }
     * ]
     * }</pre>
     *
     * <p><strong>Performance note:</strong> This endpoint returns all gift cards without
     * pagination. For production systems with large datasets, consider implementing
     * pagination parameters.</p>
     *
     * @return CompletableFuture containing a list of all gift card summaries
     */
    @GetMapping
    public CompletableFuture<List<GiftCardSummary>> getAllGiftCards() {
        return queryGateway.query(new FindAllGiftCardsQuery(), GiftCardSummaryList.class)
                .thenApply(GiftCardSummaryList::giftCards);
    }

    /**
     * Retrieves a specific gift card by its unique identifier.
     *
     * <p>This endpoint queries the read model for a single gift card, returning its
     * current state including remaining balance and original value. Returns HTTP 404
     * if the gift card is not found.</p>
     *
     * <p><strong>HTTP mapping:</strong> {@code GET /api/giftcards/{giftCardId}}</p>
     *
     * <p><strong>Path variable:</strong> {@code giftCardId} - the unique identifier of the gift card</p>
     *
     * <p><strong>Success response:</strong> HTTP 200 with gift card summary JSON</p>
     * <pre>{@code
     * {
     *   "giftCardId": "uuid-123",
     *   "remainingValue": 25.00,
     *   "initialValue": 50.00
     * }
     * }</pre>
     *
     * <p><strong>Not found response:</strong> HTTP 404 when gift card doesn't exist</p>
     *
     * @param giftCardId the unique identifier of the gift card to retrieve
     * @return CompletableFuture containing ResponseEntity with gift card summary or 404 status
     */
    @GetMapping("/{giftCardId}")
    public CompletableFuture<ResponseEntity<GiftCardSummary>> getGiftCard(@PathVariable("giftCardId") String giftCardId) {
        return queryGateway.query(new FindGiftCardQuery(giftCardId), GiftCardSummary.class)
                .thenApply(giftCard -> giftCard != null
                        ? ResponseEntity.ok(giftCard)
                        : ResponseEntity.notFound().build());
    }

    /**
     * Provides real-time updates for a specific gift card via Server-Sent Events (SSE).
     *
     * <p>This endpoint establishes a persistent connection that streams gift card updates
     * in real-time. Clients receive the initial gift card state immediately, followed by
     * live updates whenever the gift card is modified (issued or redeemed).</p>
     *
     * <p><strong>HTTP mapping:</strong> {@code GET /api/giftcards/{giftCardId}/updates}</p>
     * <p><strong>Content-Type:</strong> {@code text/event-stream}</p>
     * <p><strong>Path variable:</strong> {@code giftCardId} - the unique identifier of the gift card</p>
     *
     * <p><strong>SSE stream format:</strong></p>
     * <pre>{@code
     * data: {"giftCardId":"uuid-123","remainingValue":25.00,"initialValue":50.00}
     *
     * data: {"giftCardId":"uuid-123","remainingValue":15.00,"initialValue":50.00}
     * }</pre>
     *
     * <p><strong>Use cases:</strong></p>
     * <ul>
     *   <li>Real-time gift card balance displays</li>
     *   <li>Live transaction monitoring</li>
     *   <li>Immediate UI updates without polling</li>
     * </ul>
     *
     * <p><strong>Connection management:</strong> The subscription is automatically closed
     * when the client disconnects, preventing resource leaks.</p>
     *
     * @param giftCardId the unique identifier of the gift card to monitor
     * @return Flux stream of gift card summary updates
     */
    @GetMapping(value = "/{giftCardId}/updates", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<GiftCardSummary> getGiftCardUpdates(@PathVariable("giftCardId") String giftCardId) {
        return Flux.from(queryGateway.subscriptionQuery(new FindGiftCardQuery(giftCardId), GiftCardSummary.class));
    }


    /**
     * Request DTO for gift card issuance operations.
     *
     * <p>This record encapsulates the data required to issue a new gift card,
     * including the initial amount and all personal data fields.</p>
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
    public record IssueRequest(
            BigDecimal amount,
            String username,
            String ownerName,
            String addressLine1,
            String addressLine2,
            String country,
            LocalDate dateOfBirth,
            Integer randomNumber
    ) {

    }

    /**
     * Request DTO for gift card redemption operations.
     *
     * <p>This record encapsulates the data required to redeem from an existing
     * gift card. The gift card ID is provided as a path parameter in the endpoint.</p>
     *
     * @param amount the amount to redeem from the gift card (must be positive and not exceed balance)
     */
    public record RedeemRequest(BigDecimal amount) {

    }
}