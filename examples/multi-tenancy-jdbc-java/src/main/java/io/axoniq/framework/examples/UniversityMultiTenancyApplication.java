/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.examples;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.examples.cli.ReplConsole;
import io.axoniq.framework.examples.faculty.FacultyModuleConfiguration;
import io.axoniq.framework.examples.infrastructure.ConfigurationProperties;
import io.axoniq.framework.examples.infrastructure.MultiTenancyConfiguration;
import io.axoniq.platform.framework.AxoniqPlatformConfiguration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;

public class UniversityMultiTenancyApplication implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(UniversityMultiTenancyApplication.class);

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            var properties = ConfigurationProperties.load();
            logger.info("properties: {}", properties);

            var app = new UniversityMultiTenancyApplication();
            var configurer = app.configurer;

            configurer.componentRegistry(cr -> cr.registerComponent(AxoniqPlatformConfiguration.class,
                                                                    properties.platform()
                                                                              .axoniqPlatformConfiguration()))
                      .componentRegistry(cr -> cr.registerComponent(Converter.class,
                                                                    cfg -> new JacksonConverter()))
                      .componentRegistry(cr -> cr.registerComponent(AxonServerConfiguration.class,
                                                                    properties.axonServerConfiguration()))
            ;
            MultiTenancyConfiguration.configure(configurer);

            FacultyModuleConfiguration.configure(configurer);

            app.run();
        } catch (Exception e) {
            logger.error("Application failed.", e);
            // System.err is on purpose, so we see the failure in REPL.
            System.err.println("Application failed: " + e.getMessage());
            exitCode = 1;
        } finally {
            System.exit(exitCode);
        }
    }


    private final EventSourcingConfigurer configurer;
    private final InputStream input;
    private final OutputStream output;
    private AxonConfiguration axonConfiguration = null;

    // System.out used for REPL
    private UniversityMultiTenancyApplication() {
        this(System.in, System.out);
    }

    private UniversityMultiTenancyApplication(InputStream input,
                                              OutputStream output) {
        logger.info("========================================");
        this.input = input;
        this.output = output;
        this.configurer = EventSourcingConfigurer.create();
    }

    @Override
    public void run() {
        logger.info("Starting application...");
        axonConfiguration = configurer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (axonConfiguration != null) {
                logger.info("Shutting down application...");
                axonConfiguration.shutdown();
                axonConfiguration = null;
            }
        }));
        new ReplConsole(axonConfiguration, input, output).run();
    }
}
