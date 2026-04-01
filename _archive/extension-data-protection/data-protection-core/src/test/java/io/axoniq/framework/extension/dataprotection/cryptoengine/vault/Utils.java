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

import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import okhttp3.OkHttpClient;
import org.jetbrains.annotations.Nullable;

import java.security.cert.CertificateException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Utility class that has a methode to create the VaultCryptoEngine and it has a factory method to produce an
 * OkHttpClient that accepts untrusted SSL certificates. This is useful to run tests against Vault with SSL (which is
 * important to test, among other things because of performance).
 * <p>
 * Code has been copied from StackOverflow, answer by user sonxurxo: https://stackoverflow.com/a/25992879/8254465
 */
public abstract class Utils {

    /**
     * Create the VaultCryptoEngine using the provided Vault address and root token.
     *
     * @param engineVersion the Vault KV engine version (1 or 2)
     * @param vaultAddress the Vault server address (e.g., "http://localhost:8200")
     * @param rootToken the Vault root token for authentication
     * @return a configured VaultCryptoEngine instance
     */
    public static VaultCryptoEngine initVaultCryptoEngine(@Nullable Integer engineVersion,
                                                          String vaultAddress,
                                                          String rootToken) {
        /* Create the OkHttpClient. */
        OkHttpClient okHttpClient = Utils.getUnsafeOkHttpClient();

        /*
         * Select the appropriate Vault path prefix based on KV engine version:
         * - KV v2 (engineVersion == 2): Uses "secret/data/" path. This is the default KV v2 engine
         *   available in Vault dev mode at the "secret/" mount.
         * - KV v1 (otherwise): Uses "secret-v1/" path. This is a separate KV v1 engine created
         *   via the init command "secrets enable -version=1 -path=secret-v1 kv".
         *
         * Both engines can coexist in the same Vault instance on different mount paths.
         */
        String prefix = (engineVersion != null && engineVersion == 2) ? "secret/data/" : "secret-v1/";
        VaultCryptoEngine cryptoEngine = new VaultCryptoEngine(okHttpClient, vaultAddress, rootToken, prefix);
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        return cryptoEngine;
    }

    private Utils() {
        throw new Error("non-instantiable class");
    }

    private static OkHttpClient getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });

            OkHttpClient okHttpClient = builder.build();
            return okHttpClient;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
