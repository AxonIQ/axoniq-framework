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

package org.axonframework.test.matchers;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * FieldFilter that delegates to an arbitrary number of other filters. This filter accepts any field that is accepted
 * by all registered filters.
 * <p/>
 * By default, all fields are accepted.
 *
 * @author Allard Buijze
 * @since 2.4.1
 */
public class MatchAllFieldFilter implements FieldFilter {

    private final List<FieldFilter> filters = new ArrayList<>();

    /**
     * Initializes a filter that accepts any field that is accepted by all given {@code filters}
     *
     * @param filters The filters to use to evaluate a given Field
     */
    public MatchAllFieldFilter(Collection<FieldFilter> filters) {
        this.filters.addAll(filters);
    }

    @Override
    public boolean accept(Field field) {
        for (FieldFilter filter : filters) {
            if (!filter.accept(field)) {
                return false;
            }
        }
        return true;
    }
}
