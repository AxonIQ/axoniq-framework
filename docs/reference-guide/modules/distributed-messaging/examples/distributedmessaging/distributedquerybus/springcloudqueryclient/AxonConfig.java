/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package distributedmessaging.distributedquerybus.springcloudqueryclient;

// tag::query-rest-client-with-ssl-bundle[]
import io.axoniq.framework.springboot.autoconfig.SpringCloudAutoConfiguration;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
public class AxonConfig {

    @Bean(SpringCloudAutoConfiguration.QUERY_REST_CLIENT_BEAN)
    public RestClient axoniqSpringCloudQueryRestClient(RestClient.Builder builder, SslBundles sslBundles) {
        HttpClient httpClient = HttpClient.newBuilder()
                                          .connectTimeout(Duration.ofSeconds(2))
                                          .sslContext(sslBundles.getBundle("cluster").createSslContext())
                                          .build();
        // No read timeout, and a connection per stream rather than a bounded pool:
        // the connector bounds every query and subscription itself.
        return builder.requestFactory(new JdkClientHttpRequestFactory(httpClient))
                      .build();
    }
}
// end::query-rest-client-with-ssl-bundle[]
