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

package org.axonframework.extension.springboot;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.extension.springboot.autoconfig.AxonServerActuatorAutoConfiguration;
import org.axonframework.extension.springboot.autoconfig.AxonServerAutoConfiguration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AxonHandlerConfigurationTest.Context.class)
@EnableAutoConfiguration(exclude = {
        JmxAutoConfiguration.class,
        WebClientAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        DataSourceAutoConfiguration.class,
        AxonServerAutoConfiguration.class,
        AxonServerActuatorAutoConfiguration.class
})
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
@Disabled("TODO #3498")
public class AxonHandlerConfigurationTest {

    @Autowired
    private QueryGateway queryGateway;

    @Autowired
    private CommandGateway commandGateway;

    @Test
    void messageRoutedToCorrectMethod() throws Exception {
        assertEquals("Command: info", commandGateway.sendAndWait("info", String.class));
        assertEquals("Query: info", queryGateway.query("info", String.class, null).get());
    }

    @SuppressWarnings("unused")
    public static class CommandAndQueryHandler {

        @CommandHandler
        public String handle(String command) {
            return "Command: " + command;
        }

        @QueryHandler
        public String query(String query) {
            return "Query: " + query;
        }

    }

    @Configuration
    public static class Context {

        @Bean
        public CommandAndQueryHandler handler() {
            return new CommandAndQueryHandler();
        }
    }
}
