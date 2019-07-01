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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.github.pustike.persist.Index;
import io.github.pustike.persist.Table;
import io.github.pustike.persist.UniqueConstraint;
import io.github.pustike.persist.metadata.ColumnType;
import io.github.pustike.persist.metadata.EntityData;
import io.github.pustike.persist.metadata.FieldData;
import io.github.pustike.persist.metadata.Schema;
import io.github.pustike.persist.utils.PersistUtils;

/**
 * Map the object based schema definition to database by creating the schema, tables, etc.
 */
public final class MappingTool {
    private final SqlQuery sqlQuery;

    private MappingTool(SqlQuery sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    /**
     * Create or update the database using the given schema metadata.
     * @param sqlQuery the sql query instance
     */
    public static void create(SqlQuery sqlQuery) {
        MappingTool mappingTool = new MappingTool(sqlQuery);
        try {
            Connection connection = sqlQuery.getConnection();
            DatabaseMetaData dbMetaData = connection.getMetaData();
            mappingTool.createSchema(dbMetaData);
            mappingTool.createTables(dbMetaData);
        } catch (SQLException e) {
            throw new RuntimeException("failed to execute the mapping tool", e);
        }
    }

    private void createSchema(DatabaseMetaData dbMetaData) throws SQLException {
        String schemaName = sqlQuery.getSchema().getName();
        if (schemaName == null) {
            return;
        }
        try (ResultSet schemas = dbMetaData.getSchemas(null, schemaName)) {
            if (!schemas.next()) {
                sqlQuery.executeUpdate("create schema if not exists " + schemaName);
            }
        }
    }

    private void createTables(DatabaseMetaData dbMetaData) throws SQLException {
        Schema schema = sqlQuery.getSchema();
        Map<String, Map<String, String[]>> schemaTableInfo = new LinkedHashMap<>();
        final String[] columnKeys = {"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
            "COLUMN_SIZE", "DECIMAL_DIGITS", "IS_NULLABLE"};
        try (ResultSet columns = dbMetaData.getColumns(null, schema.getName(), null, null)) {
            while (columns.next()) {
                String tableName = columns.getString(columnKeys[0]);
                Map<String, String[]> tableInfo = schemaTableInfo.computeIfAbsent(tableName, k -> new LinkedHashMap<>());
                String columnName = columns.getString(columnKeys[1]);
                String[] columnValues = new String[columnKeys.length - 2];
                for (int i = 2; i < columnKeys.length; i++) {
                    columnValues[i - 2] = columns.getString(columnKeys[i]);
                }
                tableInfo.put(columnName, columnValues);
            }
        }
        List<ForeignKey> foreignKeyList = new ArrayList<>();
        for (EntityData entityData : schema.getEntityData()) {
            if (entityData.isSuperClass()) {
                continue;// this is an abstract/mapped super class!
            }
            final String tableName = entityData.getTableName();
            Collection<FieldData> fieldDataList = entityData.getFieldData();
            Map<String, String[]> tableInfo = schemaTableInfo.get(tableName);
            if (tableInfo == null) {// table is not present!
                createTable(schema, entityData, fieldDataList);
            } else {
                List<FieldData> missingFieldDataList = new ArrayList<>();
                for (FieldData fieldData : fieldDataList) {
                    if (!tableInfo.containsKey(fieldData.getColumnName())) {
                        missingFieldDataList.add(fieldData);
                    } else {
                        // TODO validate if the column definition is matching with the fieldMetadata!
                    }
                }
                if (!missingFieldDataList.isEmpty()) {
                    alterTable(schema, entityData, missingFieldDataList);
                }
            }
            createIndexes(schema, dbMetaData, entityData, fieldDataList);
            for (FieldData fmd : fieldDataList) {
                if (fmd.getColumnType() == ColumnType.ForeignKey) {
                    String fkName = tableName + "_" + fmd.getColumnName() + "_fkey";
                    String targetClassName = fmd.getFieldType().getName();
                    EntityData targetEntity = schema.getEntityData(targetClassName);
                    if (targetEntity == null) {
                        throw new IllegalArgumentException("Foreign key table doesn't exist for: " + targetClassName);
                    }
                    foreignKeyList.add(new ForeignKey(fkName, tableName, fmd.getColumnName(),
                        targetEntity.getTableName(), targetEntity.getIdField().getColumnName()));
                }
            }
        }
        Map<String, ForeignKey> existingForeignKeyMap = new HashMap<>();
        try (ResultSet rs = dbMetaData.getExportedKeys(null, schema.getName(), null)) {
            while (rs.next()) {
                String fkName = rs.getString("FK_NAME");
                String fkTableName = rs.getString("FKTABLE_NAME");
                String fkColumnName = rs.getString("FKCOLUMN_NAME");
                String pkTableName = rs.getString("PKTABLE_NAME");
                String pkColumnName = rs.getString("PKCOLUMN_NAME");
                existingForeignKeyMap.put(fkName, new ForeignKey(fkName, fkTableName, fkColumnName,
                    pkTableName, pkColumnName));
            }
        }
        for (ForeignKey fk : foreignKeyList) {
            if (!existingForeignKeyMap.containsKey(fk.getName())) { // create foreign keys
                String queryBuilder = "ALTER TABLE " + toSchemaTableName(schema, fk.getTableName())
                    + " ADD CONSTRAINT " + fk.getName() + " FOREIGN KEY (" + fk.getColumnName()
                    + ") REFERENCES " + toSchemaTableName(schema, fk.getTargetTable())
                    + " (" + fk.getTargetColumn() + ") DEFERRABLE INITIALLY DEFERRED";
                sqlQuery.executeUpdate(queryBuilder);
            }
        }
    }

    private void createTable(Schema schema, EntityData entityData, Collection<FieldData> fieldDataList) {
        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE ").append(schema.toSchemaTableName(entityData));
        queryBuilder.append(" (").append(fieldDataList.stream().map(FieldData::getColumnDefinition)
            .collect(Collectors.joining(", "))).append(')');
        sqlQuery.executeUpdate(queryBuilder.toString());
    }

    private void alterTable(Schema schema, EntityData entityData, List<FieldData> fieldDataList) {
        StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ").append(schema.toSchemaTableName(entityData));
        for (FieldData fieldData : fieldDataList) {
            queryBuilder.append(" ADD COLUMN ").append(fieldData.getColumnDefinition()).append(',');
        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        sqlQuery.executeUpdate(queryBuilder.toString());
    }

    private void createIndexes(Schema schema, DatabaseMetaData dbMetaData, EntityData entityData,
        Collection<FieldData> fieldDataList) throws SQLException {
        String tableName = entityData.getTableName();
        Map<String, IndexInfo> entityIndexInfoMap = new HashMap<>();
        Table table = entityData.getEntityClass().getDeclaredAnnotation(Table.class);
        UniqueConstraint[] uniqueConstraints = table.uniqueConstraints();
        for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
            String indexName = getIndexName(tableName, uniqueConstraint.name(), uniqueConstraint.columns(), true);
            IndexInfo.Builder builder = IndexInfo.create(indexName).on(tableName).unique(true);
            for (String fieldName : uniqueConstraint.columns()) {
                FieldData fieldData = entityData.getFieldData(fieldName);
                if (fieldData == null) {
                    throw new IllegalArgumentException("invalid column name used in unique constraint on table: "
                        + tableName + ", field: " + fieldName);
                }
                builder.add(fieldData.getColumnName());
            }
            entityIndexInfoMap.put(indexName, builder.build());
        }
        Index[] indices = entityData.getEntityClass().getDeclaredAnnotationsByType(Index.class);
        if (indices != null) {
            for (Index index : indices) {
                String indexName = getIndexName(tableName, index.name(), index.columns(), false);
                IndexInfo.Builder builder = IndexInfo.create(indexName).on(tableName);
                for (String fieldName : index.columns()) {
                    FieldData fieldData = entityData.getFieldData(fieldName);
                    if (fieldData == null) {
                        throw new IllegalArgumentException("invalid column name used in index on table: " + tableName);
                    }
                    builder.add(fieldData.getColumnName());
                }
                entityIndexInfoMap.put(indexName, builder.build());
            }
        }
        for (FieldData fmd : fieldDataList) {
            Index index = fmd.getField().getDeclaredAnnotation(Index.class);
            if (index != null) {
                String indexName = tableName + "_" + fmd.getColumnName() + "_idx";
                IndexInfo.Builder builder = IndexInfo.create(indexName).on(tableName).add(fmd.getColumnName());
                entityIndexInfoMap.put(indexName, builder.build());
            }
        }
        Map<String, IndexInfo.Builder> indexInfoMap = new HashMap<>();
        try (ResultSet rs = dbMetaData.getIndexInfo(null, schema.getName(), tableName, false, true)) {
            while (rs.next()) {
                String indexName = rs.getString("index_name");
                boolean isNonUnique = rs.getBoolean("NON_UNIQUE");
                indexInfoMap.computeIfAbsent(indexName, s -> IndexInfo.create(indexName).on(tableName)
                    .unique(!isNonUnique)).add(rs.getString("column_name"));
            }
        }
        for (Map.Entry<String, IndexInfo> mapEntry : entityIndexInfoMap.entrySet()) {
            String indexName = mapEntry.getKey();
            if (!indexInfoMap.containsKey(indexName)) {
                IndexInfo indexInfo = mapEntry.getValue();
                if (indexInfo.isUnique()) {
                    String queryString = "ALTER TABLE " + toSchemaTableName(schema, tableName) + " ADD CONSTRAINT "
                        + indexName + " UNIQUE (" + String.join(", ", indexInfo.getColumns()) + ')';
                    sqlQuery.executeUpdate(queryString);
                } else {
                    String queryString = "CREATE INDEX " + indexName + " ON " + toSchemaTableName(schema, tableName)
                        + " (" + String.join(", ", indexInfo.getColumns()) + ')';
                    sqlQuery.executeUpdate(queryString);
                }
            }
        }
    }

    private String toSchemaTableName(Schema schema, String tableName) {
        String schemaName = schema.getName();
        return schemaName == null ? tableName : schemaName + '.' + tableName;
    }

    private String getIndexName(String tableName, String indexName, String[] columns, boolean unique) {
        String name = indexName.trim();
        if (name.isEmpty()) {
            int hashCode = String.join("", columns).hashCode();
            name = PersistUtils.hashCodeToString(hashCode) + (unique ? "_key" : "_idx"); // _key for unique
        }
        return tableName + "_" + name;
    }
}
