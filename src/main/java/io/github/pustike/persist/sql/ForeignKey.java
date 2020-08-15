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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

final class ForeignKey {
    private final String name;
    private final String tableName;
    private final String columnName;
    private final String targetTable;
    private final String targetColumn;

    ForeignKey(String name, String tableName, String columnName, String targetTable, String targetColumn) {
        this.name = Objects.requireNonNull(name);
        this.tableName = tableName;
        this.columnName = columnName;
        this.targetTable = targetTable;
        this.targetColumn = targetColumn;
    }

    ForeignKey(ResultSet rs) throws SQLException {
        this(rs.getString("FK_NAME"), rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
                rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME"));
    }

    String getName() {
        return name;
    }

    String getTableName() {
        return tableName;
    }

    String getColumnName() {
        return columnName;
    }

    String getTargetTable() {
        return targetTable;
    }

    String getTargetColumn() {
        return targetColumn;
    }
}
