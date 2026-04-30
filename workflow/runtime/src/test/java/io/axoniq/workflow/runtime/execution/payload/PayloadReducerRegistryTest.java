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
package io.axoniq.workflow.runtime.execution.payload;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME_COMBINE_GLOBAL_AND_LOCAL;
import static io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME_GLOBAL_ONLY;
import static io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer.NAME_LOCAL_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

class PayloadReducerRegistryTest {

    @Test
    void loadsBuiltInReducersViaSpi() {
        var registry = new PayloadReducerRegistry();

        var global = Map.<String, Object>of("shared", "global", "globalOnly", true);
        var local = Map.<String, Object>of("shared", "local", "localOnly", true);

        assertThat(registry.get(NAME_LOCAL_ONLY))
                .hasValueSatisfying(reducer -> {
                    assertThat(reducer).isInstanceOf(LocalOnlyPayloadReducer.class);
                    assertThat(reducer.apply(global, local)).isSameAs(local);
                });
        assertThat(registry.get(NAME_GLOBAL_ONLY))
                .hasValueSatisfying(reducer -> {
                    assertThat(reducer).isInstanceOf(GlobalOnlyPayloadReducer.class);
                    assertThat(reducer.apply(global, local)).isSameAs(global);
                });
        assertThat(registry.get(NAME_COMBINE_GLOBAL_AND_LOCAL))
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

        assertThat(registry.get(NAME_LOCAL_ONLY)).isEmpty();
        assertThat(registry.get(NAME_GLOBAL_ONLY)).isEmpty();
        assertThat(registry.get(NAME_COMBINE_GLOBAL_AND_LOCAL)).isEmpty();
    }
}
