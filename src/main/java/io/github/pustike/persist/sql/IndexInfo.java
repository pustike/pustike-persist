/*
 * Copyright (C) 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.pustike.persist.sql;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class IndexInfo {
    private final String indexName;
    private final List<String> columns;
    private final boolean unique;

    public IndexInfo(String indexName, List<String> columns) {
        this(indexName, columns, false);
    }

    public IndexInfo(String indexName, List<String> columns, boolean unique) {
        this.indexName = Objects.requireNonNull(indexName);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("at least one column should be specified for index: " + indexName);
        }
        this.columns = Collections.unmodifiableList(columns);
        this.unique = unique;
    }

    String getIndexName() {
        return indexName;
    }

    List<String> getColumns() {
        return columns;
    }

    boolean isUnique() {
        return unique;
    }
}
