/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package tracing.exporters;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class SpringExporterConfiguration {

    // tag::additional-exporter[]
    @Bean
    SpanExporter secondarySpanExporter() {
        return OtlpGrpcSpanExporter.builder()
                                   .setEndpoint("http://secondary-collector:4317")
                                   .build();
    }
    // end::additional-exporter[]
}
