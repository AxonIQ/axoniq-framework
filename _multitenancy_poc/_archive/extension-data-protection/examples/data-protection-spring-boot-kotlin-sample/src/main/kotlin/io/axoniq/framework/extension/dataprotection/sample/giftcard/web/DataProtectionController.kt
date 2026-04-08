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

import io.axoniq.framework.extension.dataprotection.sample.giftcard.event.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.concurrent.CompletableFuture

/**
 * REST controller for data protection operations (GDPR compliance).
 *
 * This controller provides endpoints for managing personal data in accordance with GDPR
 * regulations, specifically implementing the "right to be forgotten" through encryption key deletion.
 *
 * Key Features:
 * - Delete encryption keys to make encrypted data unrecoverable
 * - Ensure projections reflect replacement values (e.g., "<removed>") for forgotten data
 *
 */
@RestController
@RequestMapping("/api/data-protection")
@CrossOrigin(origins = ["*"])
class DataProtectionController(private val cryptoEngine: CryptoEngine) {

    private val logger = LoggerFactory.getLogger(DataProtectionController::class.java)

    /**
     * Delete encryption key for a specific data subject (GDPR "right to be forgotten").
     *
     * This endpoint implements the GDPR "right to be forgotten" by deleting the encryption key
     * associated with a specific data subject. Once the key is deleted:
     * 1. The encrypted data becomes permanently unrecoverable
     * 2. Projections will display replacement values (e.g., "<removed>") for the encrypted fields
     *
     * Important: This operation is irreversible. The encryption key cannot be
     * recovered once deleted, and the original data will be permanently lost.
     *
     * HTTP Mapping: DELETE /api/data-protection/subjects/{dataSubjectId}
     *
     * Example:
     * curl -X DELETE http://localhost:8080/api/data-protection/subjects/abc-123-def
     *
     * @param dataSubjectId the unique identifier of the data subject whose encryption key should be deleted
     * @return CompletableFuture with ResponseEntity containing success or error message
     */
    @DeleteMapping("/subjects/{dataSubjectId}")
    fun deleteDataSubjectKey(@PathVariable dataSubjectId: String): CompletableFuture<ResponseEntity<String>> {
        logger.info("Received request to delete encryption key for data subject: {}", dataSubjectId)

        return GlobalScope.future {
            try {
                withContext(Dispatchers.IO) {
                    // Step 1: Delete the encryption key
                    val keyId = "${GiftPersonalDataGroup.GROUP_PREFIX}$dataSubjectId"
                    logger.info("Deleting encryption key: {}", keyId)
                    cryptoEngine.deleteKey(keyId)
                    logger.info("Encryption key deleted successfully: {}", keyId)
                }

                ResponseEntity.ok(
                    "Encryption key deleted successfully for data subject: $dataSubjectId. " +
                            "Refresh the page to see updated data."
                )
            } catch (e: Exception) {
                logger.error("Failed to delete encryption key for data subject: {}", dataSubjectId, e)
                ResponseEntity.badRequest().body("Failed to delete key: ${e.message}")
            }
        }
    }
}
