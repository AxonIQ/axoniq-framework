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
package io.axoniq.framework.extension.dataprotection.internal.model;

import lombok.Getter;
import lombok.NonNull;

import java.util.List;

/**
 * Model class containing all personal data field information for a class.
 *
 * @author Frans van Buul
 */
@Getter
public class FieldEncryptionModel {
    @NonNull final List<DataSubjIdField> dataSubjIdFields;
    @NonNull final List<IPDField> ipdFields;
    @NonNull final List<DPDField> dpdFields;
    @NonNull final List<SPDField> spdFields;
    @NonNull final List<MPDField> mpdFields;
    final boolean empty;

    public FieldEncryptionModel(List<DataSubjIdField> dataSubjIdFields, List<IPDField> ipdFields,
                                List<DPDField> dpdFields, List<SPDField> spdFields, List<MPDField> mpdFields) {
        this.dataSubjIdFields = dataSubjIdFields;
        this.ipdFields = ipdFields;
        this.dpdFields = dpdFields;
        this.spdFields = spdFields;
        this.mpdFields = mpdFields;
        this.empty = dataSubjIdFields.isEmpty() && ipdFields.isEmpty() && dpdFields.isEmpty()
                && spdFields.isEmpty() && mpdFields.isEmpty();
    }

}
