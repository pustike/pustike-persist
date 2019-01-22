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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class IndexInfo {
    private final String indexName;
    private String tableName;
    private List<String> columns;
    private boolean unique;

    private IndexInfo(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
        this.columns = new ArrayList<>();
    }

    static Builder create(String indexName) {
        return new Builder(indexName);
    }

    String getIndexName() {
        return indexName;
    }

    String getTableName() {
        return tableName;
    }

    boolean isUnique() {
        return unique;
    }

    List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public String toString() {
        return String.format("[%s] %s: %s", tableName, indexName, columns);
    }

    static final class Builder {
        private final IndexInfo indexInfo;

        private Builder(String indexName) {
            this.indexInfo = new IndexInfo(indexName);
        }

        Builder on(String tableName) {
            indexInfo.tableName = Objects.requireNonNull(tableName);
            return this;
        }

        Builder unique(boolean isUnique) {
            indexInfo.unique = isUnique;
            return this;
        }

        Builder add(String column) {
            indexInfo.columns.add(column);
            return this;
        }

        IndexInfo build() {
            if (indexInfo.columns.size() == 0) {
                throw new IllegalArgumentException("at least one column should be specified");
            }
            return indexInfo;
        }
    }
}
