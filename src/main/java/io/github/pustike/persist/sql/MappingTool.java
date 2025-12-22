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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                    foreignKeyList.add(new ForeignKey(fkName, tableName, fmd.getColumnName(),
                        targetEntity.getTableName(), targetEntity.getIdField().getColumnName()));
                }
            }
        }
        createForeignKeys(dbMetaData, schema, foreignKeyList);
    }

    private void createForeignKeys(DatabaseMetaData dbMetaData, Schema schema, List<ForeignKey> foreignKeyList)
            throws SQLException {
        Set<String> existingFkNames = new HashSet<>();
        try (ResultSet rs = dbMetaData.getExportedKeys(null, schema.getName(), null)) {
            while (rs.next()) {
                existingFkNames.add(new ForeignKey(rs).getName());
            }
        }
        foreignKeyList.stream().filter(fk -> !existingFkNames.contains(fk.getName())).forEach(fk -> {
            String queryBuilder = "ALTER TABLE " + toSchemaTableName(schema, fk.getTableName())
                    + " ADD CONSTRAINT " + fk.getName() + " FOREIGN KEY (" + fk.getColumnName()
                    + ") REFERENCES " + toSchemaTableName(schema, fk.getTargetTable())
                    + " (" + fk.getTargetColumn() + ") DEFERRABLE INITIALLY DEFERRED";
            sqlQuery.executeUpdate(queryBuilder);
        });
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
            List<String> columnNames = Stream.of(uniqueConstraint.columns()).map(fieldName ->
                    entityData.getFieldData(fieldName).getColumnName()).collect(Collectors.toList());
            entityIndexInfoMap.put(indexName, new IndexInfo(indexName, columnNames, true));
        }
        Index[] indices = entityData.getEntityClass().getDeclaredAnnotationsByType(Index.class);
        for (Index index : indices) {
            String indexName = getIndexName(tableName, index.name(), index.columns(), false);
            List<String> columnNames = Stream.of(index.columns()).map(fieldName ->
                    entityData.getFieldData(fieldName).getColumnName()).collect(Collectors.toList());
            entityIndexInfoMap.put(indexName, new IndexInfo(indexName, columnNames));
        }
        for (FieldData fieldData : fieldDataList) {
            Index index = fieldData.getField().getDeclaredAnnotation(Index.class);
            if (index != null) {
                String indexName = tableName + "_" + fieldData.getColumnName() + "_idx";
                entityIndexInfoMap.put(indexName, new IndexInfo(indexName, List.of(fieldData.getColumnName())));
            }
        }
        Set<String> indexNameSet = new HashSet<>();
        try (ResultSet rs = dbMetaData.getIndexInfo(null, schema.getName(), tableName, false, true)) {
            while (rs.next()) {
                String indexName = rs.getString("index_name");
                // boolean isNonUnique = rs.getBoolean("NON_UNIQUE");// String columnName = rs.getString("column_name");
                indexNameSet.add(indexName);
            }
        }
        entityIndexInfoMap.entrySet().stream().filter(entry -> !indexNameSet.contains(entry.getKey()))
                .forEach(mapEntry -> createIndex(schema, tableName, mapEntry.getValue()));
    }

    private void createIndex(Schema schema, String tableName, IndexInfo indexInfo) {
        String queryString;
        if (indexInfo.unique()) {
            queryString = "ALTER TABLE " + toSchemaTableName(schema, tableName) + " ADD CONSTRAINT "
                    + indexInfo.indexName() + " UNIQUE (" + String.join(", ", indexInfo.columns()) + ')';
        } else {
            queryString = "CREATE INDEX " + indexInfo.indexName() + " ON " + toSchemaTableName(schema, tableName)
                    + " (" + String.join(", ", indexInfo.columns()) + ')';
        }
        sqlQuery.executeUpdate(queryString);
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

    private record IndexInfo(String indexName, List<String> columns, boolean unique) {
        public IndexInfo(String indexName, List<String> columns) {
            this(indexName, columns, false);
        }

        private IndexInfo(String indexName, List<String> columns, boolean unique) {
            this.indexName = Objects.requireNonNull(indexName);
            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("at least one column should be specified for index: " + indexName);
            }
            this.columns = Collections.unmodifiableList(columns);
            this.unique = unique;
        }
    }
}
