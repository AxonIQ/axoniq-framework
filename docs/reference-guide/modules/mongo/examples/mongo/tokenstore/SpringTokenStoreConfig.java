/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package mongo.tokenstore;

// tag::spring-token-store[]
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class SpringTokenStoreConfig {

    @Bean
    TokenStore tokenStore(MongoTemplate mongoTemplate,
                          MongoTransactionalExecutorProvider executorProvider,
                          GeneralConverter converter) {
        return new MongoTokenStore(
                mongoTemplate.trackingTokensCollection(),
                executorProvider,
                converter,
                MongoTokenStoreConfiguration.DEFAULT.claimTimeout(Duration.ofSeconds(30))
        );
    }
}
// end::spring-token-store[]
