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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AxonServerTenantUtilsTest {

    @Nested
    class TenantDescriptorExtraction {

        @Test
        void usesContextNameAsTenantId() {
            // given
            ContextOverview contextOverview = contextOverview("tenant-a", "default-rg");

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.tenantId()).isEqualTo("tenant-a");
        }

        @Test
        void copiesContextMetadataIntoTenantProperties() {
            // given
            ContextOverview contextOverview = contextOverview(
                    "tenant-a",
                    "default-rg",
                    Map.of(
                            "region", "eu-west",
                            "tier", "gold"
                    )
            );

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.properties()).containsEntry("region", "eu-west")
                                           .containsEntry("tier", "gold");
        }

        @Test
        void addsReplicationGroupFromContextWhenMetadataDoesNotProvideIt() {
            // given
            ContextOverview contextOverview = contextOverview(
                    "tenant-a",
                    "default-rg",
                    Map.of("region", "eu-west")
            );

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.properties()).containsEntry("replicationGroup", "default-rg")
                                           .containsEntry("region", "eu-west");
        }

        @Test
        void keepsReplicationGroupFromMetadataWhenAlreadyPresent() {
            // given
            ContextOverview contextOverview = contextOverview(
                    "tenant-a",
                    "default-rg",
                    Map.of("replicationGroup", "metadata-rg")
            );

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.properties()).containsEntry("replicationGroup", "metadata-rg");
        }
    }

    private static ContextOverview contextOverview(String contextName,
                                                   String replicationGroup) {
        return contextOverview(contextName, replicationGroup, Map.of());
    }

    private static ContextOverview contextOverview(String contextName,
                                                   String replicationGroup,
                                                   Map<String, String> metadata) {
        return ContextOverview.newBuilder()
                              .setName(contextName)
                              .setReplicationGroup(
                                      ReplicationGroupOverview.newBuilder()
                                                              .setName(replicationGroup)
                                                              .build()
                              )
                              .putAllMetaData(metadata)
                              .build();
    }
}
