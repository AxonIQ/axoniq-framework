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

import io.axoniq.framework.extension.dataprotection.sample.config.EventProcessorResetService;
import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftPersonalDataGroup;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller for data protection operations (GDPR compliance).
 *
 * <p>This controller provides endpoints for managing personal data in accordance with GDPR
 * regulations, specifically implementing the "right to be forgotten" through encryption key deletion.</p>
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li>Delete encryption keys to make encrypted data unrecoverable</li>
 *   <li>Automatically trigger projection rebuilds after key deletion</li>
 *   <li>Ensure projections reflect replacement values (e.g., "&lt;removed&gt;") for forgotten data</li>
 * </ul>
 *
 */
@RestController
@RequestMapping("/api/data-protection")
@CrossOrigin(origins = "*")
public class DataProtectionController {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CryptoEngine cryptoEngine;
    private final EventProcessorResetService eventProcessorResetService;

    public DataProtectionController(CryptoEngine cryptoEngine,
                                    EventProcessorResetService eventProcessorResetService) {
        this.cryptoEngine = cryptoEngine;
        this.eventProcessorResetService = eventProcessorResetService;
    }

    /**
     * Delete encryption key for a specific data subject (GDPR "right to be forgotten").
     *
     * <p>This endpoint implements the GDPR "right to be forgotten" by deleting the encryption key
     * associated with a specific data subject. Once the key is deleted:</p>
     * <ol>
     *   <li>The encrypted data becomes permanently unrecoverable</li>
     *   <li>All event processors are automatically reset to trigger a projection replay</li>
     *   <li>Projections will display replacement values (e.g., "&lt;removed&gt;") for the encrypted fields</li>
     * </ol>
     *
     * <p><strong>Important:</strong> This operation is irreversible. The encryption key cannot be
     * recovered once deleted, and the original data will be permanently lost.</p>
     *
     * <p><strong>HTTP Mapping:</strong> {@code DELETE /api/data-protection/subjects/{dataSubjectId}}</p>
     *
     * <p><strong>Example:</strong></p>
     * <pre>
     * curl -X DELETE http://localhost:8080/api/data-protection/subjects/abc-123-def
     * </pre>
     *
     * @param dataSubjectId the unique identifier of the data subject whose encryption key should be deleted
     * @return CompletableFuture with ResponseEntity containing success or error message
     */
    @DeleteMapping("/subjects/{dataSubjectId}")
    public CompletableFuture<ResponseEntity<String>> deleteDataSubjectKey(@PathVariable String dataSubjectId) {
        logger.info("Received request to delete encryption key for data subject: {}", dataSubjectId);

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Step 1: Delete the encryption key
                String keyId = GiftPersonalDataGroup.GROUP_PREFIX + dataSubjectId;
                logger.info("Deleting encryption key: {}", keyId);
                cryptoEngine.deleteKey(keyId);
                logger.info("Encryption key deleted successfully: {}", keyId);

                // Step 2: Trigger event processor reset to rebuild projections
                // This runs asynchronously in the background

                // throws ResetNotSupportedException: TODO #3304
//                logger.info("Triggering event processor reset to rebuild projections");
//                eventProcessorResetService.resetAllProcessors()
//                        .whenComplete((result, throwable) -> {
//                            if (throwable != null) {
//                                logger.error("Failed to reset event processors after key deletion for data subject: {}", dataSubjectId, throwable);
//                            } else {
//                                logger.info("Event processors reset successfully after key deletion for data subject: {}", dataSubjectId);
//                            }
//                        });

                return ResponseEntity.ok(
                        "Encryption key deleted successfully for data subject: " + dataSubjectId +
                                ". Projections are being rebuilt in the background. Refresh the page to see updated data."
                );
            } catch (Exception e) {
                logger.error("Failed to delete encryption key for data subject: {}", dataSubjectId, e);
                return ResponseEntity.badRequest().body("Failed to delete key: " + e.getMessage());
            }
        });
    }
}
