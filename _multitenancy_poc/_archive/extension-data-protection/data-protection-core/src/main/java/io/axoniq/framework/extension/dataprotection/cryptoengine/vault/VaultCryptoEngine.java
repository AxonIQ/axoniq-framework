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
package io.axoniq.framework.extension.dataprotection.cryptoengine.vault;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.DatabaseBackedCryptoEngine;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * HashiCorp Vault-based implementation of the {@link CryptoEngine} interface.
 *
 * @author Frans van Buul
 */
public class VaultCryptoEngine extends DatabaseBackedCryptoEngine {

    private static final MediaType MEDIA_TYPE_APPLICATION_JSON
            = MediaType.parse("application/json; charset=utf-8");

    private final OkHttpClient okHttpClient;
    private final String address;
    private String token;
    private final String prefix;
    private final String propertyName;

    /**
     * Instantiate a new VaultCryptoEngine, using 'key' as the property name.
     *
     * @param okHttpClient the OkHttpClient to use
     * @param address      the URL of the Vault server
     * @param token        the token to be used initially
     * @param prefix       the prefix to use in the Vault namespace; could be "secret/" in a simple test, but probably
     *                     something more specific in a real-life scenario
     */
    public VaultCryptoEngine(OkHttpClient okHttpClient, String address, String token, String prefix) {
        this(okHttpClient, address, token, prefix, "key");
    }

    /**
     * Instantiate a new VaultCryptoEngine.
     *
     * @param okHttpClient the OkHttpClient to use
     * @param address      the URL of the Vault server
     * @param token        the token to be used initially
     * @param prefix       the prefix to use in the Vault namespace; could be "secret/" in a simple test, but probably
     *                     something more specific in a real-life scenario
     * @param propertyName the property to be used to store the AES key.
     */
    public VaultCryptoEngine(OkHttpClient okHttpClient,
                             String address,
                             String token,
                             String prefix,
                             String propertyName) {
        this.okHttpClient = okHttpClient;
        this.address = address;
        this.token = token;
        this.prefix = prefix;
        this.propertyName = propertyName;
    }

    /**
     * Set the Vault token to be used in subsequent requests.
     * @param token the new token
     */
    public void setToken(String token) {
        this.token = token;
    }

    @Override
    protected SecretKey putKeyIfAbsent(String id, SecretKeySpec secretKeySpec) {
        int count = 0;
        SecretKey secretKey = getKey(id);
        while(secretKey == null) {
            try {
                putKey(id, secretKeySpec);
                secretKey = secretKeySpec;
            } catch(PermissionDeniedException ex) {
                if(count < 5) {
                    secretKey = getKey(id);
                } else {
                    throw new RuntimeException(ex);
                }
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
            count++;
        }
        return secretKey;
    }

    /**
     * Tries to put a key in Vault. Will throw an exception if this fails, in
     * particular a PermissionDeniedException when receiving a 403 response. This
     * should occur when there is an attempt to overwrite a key.
     *
     * There is no need to call this method directly from the application. It
     * is made public to enable testing of Vault policies.
     *
     * @param id the id of the key
     * @param secretKeySpec the key data itself
     * @throws IOException if it can't write the key
     */
    public void putKey(String id, SecretKeySpec secretKeySpec) throws IOException {
        String url = null;
        try {
            url = new StringBuilder()
                    .append(address)
                    .append("/v1/")
                    .append(prefix)
                    .append(URLEncoder.encode(id, "UTF-8"))
                    .toString();
        } catch (UnsupportedEncodingException ex) {
            throw new Error("UTF-8 not supported", ex);
        }
        String body = new StringBuilder()
                .append("{\"data\":{\"")
                .append(propertyName)
                .append("\":\"")
                .append(Base64.getEncoder().encodeToString(secretKeySpec.getEncoded()))
                .append("\"}}")
                .toString();
        Request request = new Request.Builder()
                .url(url)
                .addHeader("X-Vault-Token", token)
                .post(RequestBody.create(MEDIA_TYPE_APPLICATION_JSON, body))
                .build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            if (response.code() == 403) throw new PermissionDeniedException(response);
            if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
        }
    }

    @Override
    public SecretKey getKey(String id) {
        String url = null;
        try {
            url = new StringBuilder()
                    .append(address)
                    .append("/v1/")
                    .append(prefix)
                    .append(URLEncoder.encode(id, "UTF-8"))
                    .toString();
        } catch (UnsupportedEncodingException ex) {
            throw new Error("UTF-8 not supported", ex);
        }
        Request request = new Request.Builder()
                .url(url)
                .addHeader("X-Vault-Token", token)
                .build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            if(response.code() == 404) {
                return null;
            }
            if (!response.isSuccessful()) throw new RuntimeException("Unexpected code " + response);
            JsonObject object = new JsonParser().parse(response.body().charStream()).getAsJsonObject();
            String keyText = object.getAsJsonObject("data").getAsJsonObject("data").get(propertyName).getAsString();
            return new SecretKeySpec(Base64.getDecoder().decode(keyText), "AES");
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void deleteKey(String id) {
        String url = null;
        try {
            url = new StringBuilder()
                    .append(address)
                    .append("/v1/")
                    .append(prefix)
                    .append(URLEncoder.encode(id, "UTF-8"))
                    .toString();
        } catch (UnsupportedEncodingException ex) {
            throw new Error("UTF-8 not supported", ex);
        }
        Request request = new Request.Builder()
                .url(url)
                .addHeader("X-Vault-Token", token)
                .delete()
                .build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new RuntimeException("Unexpected code " + response);
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
