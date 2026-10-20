/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution.payload;

import org.junit.jupiter.api.*;

import java.util.Map;

import static io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME;
import static org.assertj.core.api.Assertions.assertThat;

class PayloadReducerRegistryTest {

    @Test
    void loadsBuiltInReducersViaSpi() {
        var registry = new PayloadReducerRegistry();

        var global = Map.<String, Object>of("shared", "global", "globalOnly", true);
        var local = Map.<String, Object>of("shared", "local", "localOnly", true);

        assertThat(registry.get(LocalOnlyPayloadReducer.NAME))
                .hasValueSatisfying(reducer -> {
                    assertThat(reducer).isInstanceOf(LocalOnlyPayloadReducer.class);
                    assertThat(reducer.apply(global, local)).isSameAs(local);
                });
        assertThat(registry.get(GlobalOnlyPayloadReducer.NAME))
                .hasValueSatisfying(reducer -> {
                    assertThat(reducer).isInstanceOf(GlobalOnlyPayloadReducer.class);
                    assertThat(reducer.apply(global, local)).isSameAs(global);
                });
        assertThat(registry.get(NAME))
                .hasValueSatisfying(reducer -> {
                    assertThat(reducer).isInstanceOf(CombineGlobalAndLocalPayloadReducer.class);
                    assertThat(reducer.apply(global, local))
                            .containsEntry("globalOnly", true)
                            .containsEntry("localOnly", true)
                            .containsEntry("shared", "local");
                });
    }

    @Test
    void returnsEmptyForUnknownReducerName() {
        var registry = new PayloadReducerRegistry();

        assertThat(registry.get("missing")).isEmpty();
    }

    @Test
    void doesNotLoadReducersWhenDisabled() {
        var registry = new PayloadReducerRegistry(false, getClass().getClassLoader());

        assertThat(registry.get(LocalOnlyPayloadReducer.NAME)).isEmpty();
        assertThat(registry.get(GlobalOnlyPayloadReducer.NAME)).isEmpty();
        assertThat(registry.get(NAME)).isEmpty();
    }
}
