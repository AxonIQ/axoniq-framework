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
package io.axoniq.framework.dataprotection.cryptoengine.vault;

import okhttp3.Response;

import java.io.IOException;

/**
 * Exception that is thrown internally in the VaultCryptoEngine when
 * a 403 response is returned from Vault. This class is made public for
 * testing purposes, but shouldn't be used directly by the application.
 *
 * @author Frans van Buul
 */
public class PermissionDeniedException extends IOException {

    private final Response response;

    /**
     * Create a new PermissionDeniedException
     *
     * @param response the response from HashiCorp Vault
     */
    public PermissionDeniedException(Response response) {
        this.response = response;
    }

    /**
     * Return the response that came from Vault
     * @return the response
     */
    public Response getResponse() {
        return response;
    }
}
