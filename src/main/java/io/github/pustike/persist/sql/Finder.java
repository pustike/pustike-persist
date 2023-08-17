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

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

import io.github.pustike.persist.metadata.ColumnType;
import io.github.pustike.persist.metadata.EntityData;
import io.github.pustike.persist.metadata.FieldData;
import io.github.pustike.persist.metadata.Schema;
import io.github.pustike.persist.utils.PersistUtils;

/**
 * The data finder api.
 * @param <E> type of the entity
 */
public final class Finder<E> {
    private static final Logger logger = System.getLogger(Finder.class.getName());
    private final SqlQuery sqlQuery;
    private final String alias;
    private final Map<String, EntityData> aliasEntityDataMap;
    private final Map<String, String> joinAliasMap;
    private StringBuilder joinClause, whereClause;
    private String groupBy, orderBy;
    private final List<Object> parameterList;

    Finder(SqlQuery sqlQuery, Class<E> entityClass, String alias) {
        this.sqlQuery = sqlQuery;
        this.alias = alias;
        this.aliasEntityDataMap = new HashMap<>();
        this.aliasEntityDataMap.put(alias, sqlQuery.getSchema().getEntityData(entityClass));
        this.parameterList = new ArrayList<>();
        this.joinClause = new StringBuilder();
        this.joinAliasMap = new HashMap<>();
    }

    /**
     * Overrides the initial entity class and creates new alias finder for the given data class
     * @param dataClass the expected data class
     * @param alias the alias
     * @param <V> the type of the value
     * @return a new alias finder
     */
    public <V> AliasFinder<V> select(Class<V> dataClass, String alias) {
        return new AliasFinder<>(this, alias);
    }

    /**
     * Add the join clause using alias.field format and also specify the new alias for this type. Field should be of
     * type: {@link ColumnType#ForeignKey} and if it is optional {@code left outer join} is used,
     * else it uses {@code inner join}.
     * @param aliasFieldName the alias.field name
     * @param asAlias the new alias for this join
     * @return this finder instance
     */
    public Finder<E> join(String aliasFieldName, String asAlias) {
        return join(aliasFieldName, asAlias, false);
    }

    /**
     * Add the join clause using alias.field format and also specify the new alias for this type. Using the parameter
     * inner join can be enforced even though the field is optional.
     * @param aliasFieldName the alias.field name
     * @param asAlias the new alias for this join
     * @param changeToInnerJoin {@code true} to use inner join even though the field is optional
     * @return this finder instance
     */
    public Finder<E> join(String aliasFieldName, String asAlias, boolean changeToInnerJoin) {
        if (aliasEntityDataMap.containsKey(asAlias)) {
            throw new IllegalArgumentException("this alias is already used: " + asAlias);
        }
        String[] strings = aliasFieldName.split("\\.");
        if (strings.length != 2) {
            throw new IllegalArgumentException("invalid usage of field: " + aliasFieldName);
        }
        String fromAlias = strings[0].trim();
        FieldData fieldData = toFieldData(strings[1].trim(), fromAlias);
        if (fieldData.getColumnType() != ColumnType.ForeignKey) {
            throw new IllegalStateException("join field is not a foreign key: " + aliasFieldName);
        }
        Schema schema = sqlQuery.getSchema();
        EntityData fkEntityData = schema.getEntityData(fieldData.getFieldType());
        aliasEntityDataMap.put(asAlias, fkEntityData);
        joinClause.append(getJoinClause(fromAlias, fkEntityData, fieldData, asAlias, changeToInnerJoin));
        joinAliasMap.put(aliasFieldName, asAlias);
        return this;
    }

    /**
     * Join the table of data class on given conditional clause.
     * @param dataClass the data class to join
     * @param alias the new alias for this join
     * @param useOuterJoin  {@code true} to use outer join else uses inner join
     * @param onCondition the conditional clause
     * @return this finder instance
     */
    public Finder<E> join(Class<?> dataClass, String alias, boolean useOuterJoin, String onCondition) {
        String joinString = (useOuterJoin ? " left outer join " : " inner join ") +
                sqlQuery.getTableName(dataClass) + " as " + alias + " on " + onCondition;
        joinClause.append(joinString);
        aliasEntityDataMap.put(alias, sqlQuery.getSchema().getEntityData(dataClass));
        return this;
    }

    private FieldData toFieldData(String fieldName, String fromAlias) {
        EntityData entityData = aliasEntityDataMap.get(fromAlias);
        if (entityData == null) {
            throw new IllegalArgumentException("the alias is not joined in this query: " + fromAlias);
        }
        return entityData.getFieldData(fieldName);
    }

    private String getJoinClause(String fromAlias, EntityData fkData, FieldData fieldData, String asAlias,
        boolean changeToInnerJoin) {
        String joinType = changeToInnerJoin || !fieldData.isOptional() ? " inner join " : " left outer join ";
        StringBuilder queryBuilder = new StringBuilder(joinType).append(sqlQuery.getSchema().toSchemaTableName(fkData))
            .append(" as ").append(asAlias).append(" on ")
            .append(asAlias).append('.').append(fkData.getIdField().getColumnName()).append(" = ");
        return queryBuilder.append(fromAlias).append('.').append(fieldData.getColumnName()).toString();
    }

    /**
     * Add the query string to whereClause and add given parameters to the list of params.
     * @param queryString the query string, can be empty
     * @param parameters array of parameters, can be empty
     * @return this finder instance
     */
    public Finder<E> where(String queryString, Object... parameters) {
        if (parameters != null && parameters.length > 0) {
            parameterList.addAll(List.of(parameters));
        }
        if (queryString != null && !queryString.isEmpty()) {
            getWhereClause().append(toSqlString(queryString));
        }
        return this;
    }

    private StringBuilder getWhereClause() {
        if (whereClause == null) {
            whereClause = new StringBuilder(" where ");
        } else {
            whereClause.append(" and ");
        }
        return whereClause;
    }

    private String toSqlString(String queryString) {
        StringBuilder queryBuilder = new StringBuilder(queryString.length());
        StringBuilder aliasBuilder = new StringBuilder(), fieldBuilder = new StringBuilder();
        String alias = null;
        for (int i = 0, length = queryString.length(); i < length; i++) {
            char c = queryString.charAt(i);
            if (alias == null) {
                if (Character.isLetterOrDigit(c)) {
                    aliasBuilder.append(c);
                } else if (c == '.') {
                    alias = aliasBuilder.toString();
                    aliasBuilder.setLength(0);
                } else {
                    if (!aliasBuilder.isEmpty()) {
                        queryBuilder.append(aliasBuilder);
                        aliasBuilder.setLength(0);
                    }
                    queryBuilder.append(c);
                }
            } else {
                if (Character.isLetterOrDigit(c)) {
                    fieldBuilder.append(c);
                } else {
                    if (!fieldBuilder.isEmpty()) {
                        FieldData fieldData = toFieldData(fieldBuilder.toString(), alias);
                        queryBuilder.append(alias).append('.').append(fieldData.getColumnName());
                        fieldBuilder.setLength(0);
                        alias = null;
                    }
                    queryBuilder.append(c);
                }
            }
        }
        if (alias != null && !fieldBuilder.isEmpty()) {
            FieldData fieldData = toFieldData(fieldBuilder.toString(), alias);
            queryBuilder.append(alias).append('.').append(fieldData.getColumnName());
        } else if (!aliasBuilder.isEmpty()) {
            queryBuilder.append(aliasBuilder);
        }
        return queryBuilder.toString();
    }

    /**
     * Add the query string + {@code (?, ...)} to whereClause and add given parameters to the list of params.
     * @param queryString the query string
     * @param parameters array of parameters
     * @return this finder instance
     */
    public Finder<E> whereIn(String queryString, Object[] parameters) {
        Objects.requireNonNull(queryString);
        if (parameters == null || parameters.length == 0) {
            throw new IllegalArgumentException("IN parameters can not be empty!");
        }
        return whereIn(queryString, List.of(parameters));
    }

    /**
     * Add the query string + {@code (?, ...)} to whereClause and add given values to the list of parameters.
     * @param queryString the query string
     * @param valueList collection of parameter values
     * @return this finder instance
     */
    public Finder<E> whereIn(String queryString, Collection<?> valueList) {
        if (valueList.isEmpty()) {
            throw new IllegalArgumentException("IN parameters can not be empty!");
        }
        StringBuilder inClause = new StringBuilder(toSqlString(queryString)).append('(');
        inClause.append("?,".repeat(valueList.size())).setLength(inClause.length() - 1);
        getWhereClause().append(inClause.append(')'));
        parameterList.addAll(valueList);
        return this;
    }

    /**
     * Add the query string + {@code (selectClause)} to whereClause using another finder instance to build inner queries
     * and it adds all parameters from the inner finder.
     * @param queryString the query string
     * @param finder the inner finder instance
     * @param selectClause the single field select clause to use for IN query
     * @return this finder instance
     */
    public Finder<E> whereIn(String queryString, Finder<?> finder, String selectClause) {
        StringBuilder inClause = new StringBuilder(toSqlString(queryString)).append('(');
        inClause.append(finder.buildInnerQueryString(selectClause));
        getWhereClause().append(inClause.append(')'));
        parameterList.addAll(finder.getParameterList());
        return this;
    }

    /**
     * Add the parameter at given index; useful when passing parameters in select clause.
     * @param index the parameter index
     * @param parameter the parameter value
     */
    public void addParameter(int index, Object parameter) {
        parameterList.add(index, parameter);
    }

    String buildInnerQueryString(String selectClause) {
        return toString("select ", toSqlString(selectClause), getFromClause(), joinClause, whereClause, groupBy);
    }

    List<Object> getParameterList() {
        return parameterList;
    }

    private String toString(CharSequence... builders) {
        StringBuilder queryBuilder = new StringBuilder();
        // Arrays.stream(builders).filter(Objects::nonNull).forEach(queryBuilder::append);
        for (CharSequence builder : builders) {
            if (builder != null && !builder.isEmpty()) {
                queryBuilder.append(builder);
            }
        }
        return queryBuilder.toString();
    }

    private StringBuilder getFromClause() {
        EntityData entityData = aliasEntityDataMap.get(alias);
        return new StringBuilder(" from ").append(sqlQuery.getSchema().toSchemaTableName(entityData))
            .append(" as ").append(alias);
    }

    /**
     * Add LIKE query string to where clause.
     * @param likeString the like string
     * @param searchByWord {@code true} if the like string should be searched by matching all words
     * @param searchColumns an array of columns to search
     * @return this finder instance
     */
    public Finder<E> like(String likeString, boolean searchByWord, String... searchColumns) {
        getWhereClause().append(likeClause(likeString, searchByWord, searchColumns));
        return this;
    }

    private String likeClause(String likeString, boolean searchByWord, String... searchColumns) {
        StringBuilder likeBuilder = new StringBuilder().append('(');
        if (!searchByWord) {
            for (int columnIndex = 0; columnIndex < searchColumns.length; columnIndex++) {
                String searchColumn = toSqlColumn(searchColumns[columnIndex]);
                likeBuilder.append(columnIndex > 0 ? " or " : "");
                likeBuilder.append("lower(").append(searchColumn).append(") like ?");
                parameterList.add('%' + likeString + '%');
            }
            return likeBuilder.append(')').toString();
        } // else split likeString into words and search for any of them!
        String[] likeStrings = likeString.split(" ");
        for (int columnIndex = 0; columnIndex < searchColumns.length; columnIndex++) {
            String searchColumn = toSqlColumn(searchColumns[columnIndex]);
            likeBuilder.append(columnIndex > 0 ? " or " : "");
            for (int likeIndex = 0; likeIndex < likeStrings.length; likeIndex++) {
                likeBuilder.append(likeIndex > 0 ? " or " : "");
                likeBuilder.append("lower(").append(searchColumn).append(") like ?");
                parameterList.add('%' + likeString + '%');
            }
        }
        // .append(") LIKE CONCAT(\"%\", ?").append(paramIndex + likeIndex).append(", \"%\")");
        return likeBuilder.append(')').toString();
    }

    private String toSqlColumn(String aliasColumn) {
        String[] strings = aliasColumn.split("\\.");
        if (strings.length != 2) {
            throw new IllegalArgumentException("invalid usage of field: " + aliasColumn);
        }
        String fromAlias = strings[0].trim();
        FieldData fieldData = toFieldData(strings[1].trim(), fromAlias);
        return fromAlias + '.' + fieldData.getColumnName();
    }

    /**
     * Set the group by clause. Can be set only once.
     * @param groupByClause the group by clause
     * @return this finder instance
     */
    public Finder<E> groupBy(String groupByClause) {
        if (this.groupBy != null) {
            throw new IllegalArgumentException("Group By clause is already specified in this finder!");
        }
        this.groupBy = " group by " + toSqlString(groupByClause);
        return this;
    }

    /**
     * Set the order by clause. Can be set only once.
     * @param orderByClause the order by clause
     * @return this finder instance
     */
    public Finder<E> orderBy(String orderByClause) {
        if (this.orderBy != null) {
            throw new IllegalArgumentException("Order By clause is already specified in this finder!");
        }
        this.orderBy = " order by " + toSqlString(orderByClause);
        return this;
    }

    /**
     * Fetch the first row data from the resultSet.
     * @return the first instance if present, else null
     */
    public E fetchFirst() {
        return fetchFirst(null);
    }

    /**
     * Fetch the first row data from the resultSet using the field group.
     * @param fieldGroup the field group to select
     * @return the first instance if present, else null
     */
    public E fetchFirst(String fieldGroup) {
        List<E> resultDataList = fetch(alias, fieldGroup, -1, 1, false);
        return resultDataList.isEmpty() ? null : resultDataList.get(0);
    }

    private <V> List<V> fetch(String alias, String fieldGroup, int offset, int limit, boolean forUpdate) {
        EntityData entityData = aliasEntityDataMap.get(alias);
        if (entityData == null) {
            throw new IllegalArgumentException("the alias is not joined in this query: " + alias);
        }
        Map<String, List<String>> aliasFieldNamesMap = new HashMap<>();
        StringBuilder joinBuilder = new StringBuilder(joinClause);
        String select = buildSelect(entityData, alias, fieldGroup, joinBuilder, aliasFieldNamesMap);
        String offsetLimit = (offset > 0 ? " offset " + offset : "") + (limit > 0 ? " limit " + limit : "");
        String queryString = toString(select, getFromClause(), joinBuilder, whereClause, groupBy, orderBy, offsetLimit);
        if (forUpdate) {
            queryString = queryString + " for update of " + alias;
        }
        logger.log(Level.DEBUG, queryString);
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryString)) {
            setParameters(stmt);
            try (ResultSet resultSet = stmt.executeQuery()) {
                List<V> resultDataList = new ArrayList<>();
                while (resultSet.next()) {
                    resultDataList.add(toResultData(resultSet, entityData.getEntityClass(), alias, aliasFieldNamesMap));
                }
                return Collections.unmodifiableList(resultDataList);
            }
        } catch (Exception e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    private String buildSelect(EntityData entityData, String alias, String fieldGroup,
        StringBuilder joinBuilder, Map<String, List<String>> aliasFieldNamesMap) {
        Schema schema = sqlQuery.getSchema();
        Set<String> fieldGroupFields = entityData.getFieldGroupFields(fieldGroup);
        int fkCounter = 0;
        List<String> selectedFieldNames = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder("select");
        for (String fieldName : fieldGroupFields) {
            int fgIndex = fieldName.indexOf('@');
            String joinFieldGroup = null;
            if (fgIndex != -1) {
                joinFieldGroup = fieldName.substring(fgIndex + 1);
                fieldName = fieldName.substring(0, fgIndex);
            }
            FieldData fieldData = entityData.getFieldData(fieldName);
            if (fieldData.getColumnType() == ColumnType.ForeignKey) {
                EntityData fkEntityData = schema.getEntityData(fieldData.getFieldType());
                String asAlias = joinAliasMap.get(alias + '.' + fieldData.getName());
                if (asAlias == null) {
                    asAlias = "t" + (fkCounter++);
                    joinBuilder.append(getJoinClause(alias, fkEntityData, fieldData, asAlias, false));
                }
                List<String> selectedFkFieldNames = new ArrayList<>();
                for (String fkFieldName : fkEntityData.getJoinFetchFields(joinFieldGroup)) {
                    int fgIndex2 = fkFieldName.indexOf('@');
                    fkFieldName = fgIndex2 == -1 ? fkFieldName : fkFieldName.substring(0, fgIndex2);
                    FieldData fkFieldData = fkEntityData.getFieldData(fkFieldName);
                    selectedFkFieldNames.add(fkFieldData.getName());
                    queryBuilder.append(' ').append(asAlias).append('.')
                        .append(fkFieldData.getColumnName()).append(',');
                }
                selectedFieldNames.add(fieldData.getName() + '@' + asAlias);
                aliasFieldNamesMap.put(fieldData.getFieldType().getSimpleName() + '@' + asAlias, selectedFkFieldNames);
            } else {
                selectedFieldNames.add(fieldData.getName());
                queryBuilder.append(' ').append(alias).append('.').append(fieldData.getColumnName()).append(',');
            }
        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        aliasFieldNamesMap.put(entityData.getEntityClass().getSimpleName() + '@' + alias, selectedFieldNames);
        return queryBuilder.toString();
    }

    private void setParameters(PreparedStatement stmt) throws SQLException {
        for (int i = 0; i < parameterList.size(); i++) {
            Object param = parameterList.get(i);
            if (param instanceof Enum<?>) {
                param = param.toString();
            }
            stmt.setObject(i + 1, param);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T toResultData(ResultSet resultSet, Class<?> entityClass, String alias,
        Map<String, List<String>> aliasFieldNamesMap) throws Exception {
        int index = 1;
        T instance = (T) entityClass.getDeclaredConstructor().newInstance();
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entityClass);
        List<String> fieldAliasList = aliasFieldNamesMap.get(entityClass.getSimpleName() + '@' + alias);
        for (String fieldAlias : fieldAliasList) {
            String[] fieldAliasSplit = fieldAlias.split("@");
            FieldData fieldData = entityData.getFieldData(fieldAliasSplit[0]);
            if (fieldData.getColumnType() == ColumnType.ForeignKey) {
                Class<?> fkFieldType = fieldData.getFieldType();
                String key = fkFieldType.getSimpleName() + '@' + fieldAliasSplit[1];
                List<String> joinFetchFields = aliasFieldNamesMap.get(key);
                EntityData fkEntityData = schema.getEntityData(fkFieldType);
                Object fkInstance = null;// TODO cache these instances based on id to avoid multiple object creations
                for (String fieldName : joinFetchFields) {
                    FieldData fkFieldData = fkEntityData.getFieldData(fieldName);
                    if (fkFieldData.getColumnType() != ColumnType.ForeignKey) {
                        Class<?> resultType = fkFieldData.getResultType();
                        Object value = resultType == null ? resultSet.getObject(index++)
                            : resultSet.getObject(index++, resultType);
                        if (value != null && fkInstance == null) {
                            fkInstance = fkEntityData.createInstance();
                        }
                        if (fkInstance != null) {
                            fkFieldData.setValue(fkInstance, value);
                        }
                    } else {// only load the id from the 2nd level foreign key!
                        Class<?> fkFieldType2 = fkFieldData.getFieldType();
                        EntityData fkEntityData2 = schema.getEntityData(fkFieldType2);
                        FieldData fkIdField2 = fkEntityData2.getIdField();
                        Class<?> resultType = fkIdField2.getResultType();
                        Object value = resultType == null ? resultSet.getObject(index++)
                            : resultSet.getObject(index++, resultType);
                        if (value != null) {
                            Object fkInstance2 = fkEntityData2.createInstance();
                            fkIdField2.setValue(fkInstance2, value);
                            fkFieldData.setValue(fkInstance, fkInstance2);
                        }
                    }
                }
                fieldData.setValue(instance, fkInstance);
            } else {
                Class<?> resultType = fieldData.getResultType();
                fieldData.setValue(instance, resultType == null ? resultSet.getObject(index++)
                    : resultSet.getObject(index++, resultType));
            }
        }
        return instance;
    }

    /**
     * Fetch the first row data from the resultSet for update.
     * @return the first instance if present, else null
     */
    public E fetchFirstForUpdate() {
        return fetchFirstForUpdate(null);
    }

    /**
     * Fetch the first row data from the resultSet using the field group, for update.
     * @param fieldGroup the field group to select
     * @return the first instance if present, else null
     */
    public E fetchFirstForUpdate(String fieldGroup) {
        List<E> resultDataList = fetch(alias, fieldGroup, -1, 1, true);
        return resultDataList.isEmpty() ? null : resultDataList.get(0);
    }

    /**
     * Fetch the list of data from the resultSet.
     * @param firstResult the offset to use
     * @param maxResults the limit to apply
     * @return the list of data
     */
    public List<E> fetch(int firstResult, int maxResults) {
        return fetch(alias, null, firstResult, maxResults, false);
    }

    /**
     * Fetch the list of data from the resultSet using the field group.
     * @param fieldGroup the field group to select
     * @param firstResult the offset to use
     * @param maxResults the limit to apply
     * @return the list of data
     */
    public List<E> fetch(String fieldGroup, int firstResult, int maxResults) {
        return fetch(alias, fieldGroup, firstResult, maxResults, false);
    }

    /**
     * Fetch the list of data from the resultSet, for update.
     * @param firstResult the offset to use
     * @param maxResults the limit to apply
     * @return the list of data
     */
    public List<E> fetchForUpdate(int firstResult, int maxResults) {
        return fetch(alias, null, firstResult, maxResults, true);
    }

    /**
     * Fetch the list of data from the resultSet using the field group, for update.
     * @param fieldGroup the field group to select
     * @param firstResult the offset to use
     * @param maxResults the limit to apply
     * @return the list of data
     */
    public List<E> fetchForUpdate(String fieldGroup, int firstResult, int maxResults) {
        return fetch(alias, fieldGroup, firstResult, maxResults, true);
    }

    /**
     * Fetch the aggregate query result data.
     * @param selectClause the select clause to apply
     * @param <T> the type of result data
     * @return the single result from the query
     */
    public <T> T fetchSingleResult(String selectClause) {
        List<T> resultDataList = doFetchResults(null, selectClause, -1, 1, null, null);
        return resultDataList.isEmpty() ? null : resultDataList.get(0);
    }

    /**
     * Fetch the aggregate query result data.
     * @param selectClause the select clause to apply
     * @param columnDataTypes data type to use per column
     * @param <T> the type of result data
     * @return the single result from the query
     */
    public <T> T fetchSingleResult(String selectClause, Map<Integer, Class<?>> columnDataTypes) {
        List<T> resultDataList = doFetchResults(null, selectClause, -1, 1, columnDataTypes, null);
        return resultDataList.isEmpty() ? null : resultDataList.get(0);
    }

    /**
     * Fetch the aggregate query result data.
     * @param selectClause the select clause to apply
     * @param queryModifier the function to be called before the query is executed
     * @param <T> the type of result data
     * @return the single result from the query
     */
    public <T> T fetchSingleResult(String selectClause, Function<String, String> queryModifier) {
        List<T> resultDataList = doFetchResults(null, selectClause, -1, -1, null, queryModifier);
        return resultDataList.isEmpty() ? null : resultDataList.get(0);
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> doFetchResults(Class<T> resultType, String selectClause, int offset, int limit,
        Map<Integer, Class<?>> columnDataTypes, Function<String, String> queryModifier) {
        Class<T> dataType = resultType != null && resultType.isArray()
            ? (Class<T>) resultType.getComponentType() : resultType;
        String offsetLimit = (offset > 0 ? " offset " + offset : "") + (limit > 0 ? " limit " + limit : "");
        String queryString = toString("select ", toSqlString(selectClause), getFromClause(),
            joinClause, whereClause, groupBy, orderBy, offsetLimit);
        if (queryModifier != null) {
            queryString = queryModifier.apply(queryString);
        }
        logger.log(Level.DEBUG, queryString);
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryString)) {
            setParameters(stmt);
            List<T> resultDataList = new ArrayList<>();
            try (ResultSet resultSet = stmt.executeQuery()) {
                ResultSetMetaData resultMetaData = resultSet.getMetaData();
                int columnCount = resultMetaData.getColumnCount();
                Map<String, Field> columnFieldMap = null;
                while (resultSet.next()) {
                    if (columnCount == 1) {
                        resultDataList.add(dataType == null ? (T) resultSet.getObject(1)
                            : resultSet.getObject(1, dataType));
                    } else {
                        if (dataType == null || resultType.isArray()) {
                            List<Object> rowDataList = new ArrayList<>();
                            for (int idx = 1; idx <= columnCount; idx++) {
                                Class<?> fieldType = dataType != null ? dataType :
                                    columnDataTypes != null ? columnDataTypes.get(idx) : null;
                                rowDataList.add(fieldType == null ? resultSet.getObject(idx)
                                    : resultSet.getObject(idx, fieldType));
                            }
                            resultDataList.add((T) rowDataList.toArray(new Object[0]));
                        } else { // it is a bean class, so map the resultData to bean instances
                            if (columnFieldMap == null) {
                                columnFieldMap = new LinkedHashMap<>();
                                for (int idx = 1; idx <= columnCount; idx++) {
                                    String columnName = resultMetaData.getColumnLabel(idx);
                                    Field field = resultType.getDeclaredField(
                                        PersistUtils.underscoreToCamelCase(columnName));
                                    if (!field.trySetAccessible()) {
                                        throw new InaccessibleObjectException(
                                            "couldn't enable access to field: " + field);
                                    }
                                    columnFieldMap.put(columnName, field);
                                }
                            }
                            T instance = resultType.getDeclaredConstructor().newInstance();
                            for (Map.Entry<String, Field> mapEntry : columnFieldMap.entrySet()) {
                                Field field = mapEntry.getValue();
                                Class<?> type = field.getType();
                                type = type.isPrimitive() ? PersistUtils.getWrapperClass(type) : type;
                                field.set(instance, resultSet.getObject(mapEntry.getKey(), type));
                            }
                            resultDataList.add(instance);
                        }
                    }
                }
            }
            return Collections.unmodifiableList(resultDataList);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    /**
     * Fetch the aggregate query result data.
     * @param resultType the class of the result type
     * @param selectClause the select clause to apply
     * @param <T> the type of result data
     * @return the single result from the query
     */
    public <T> T fetchSingleResult(Class<T> resultType, String selectClause) {
        List<T> resultDataList = doFetchResults(resultType, selectClause, -1, 1, null, null);
        return resultDataList.isEmpty() ? null : resultDataList.get(0);
    }

    /**
     * Fetch the list of data from the resultSet by applying the offset and limit.
     * @param selectClause the select clause to apply
     * @param offset the offset to use
     * @param limit the limit to apply
     * @param <T> the type of result data
     * @return the list of data
     */
    public <T> List<T> fetchResults(String selectClause, int offset, int limit) {
        return doFetchResults(null, selectClause, offset, limit, null, null);
    }

    /**
     * Fetch the list of data from the resultSet by applying the offset and limit.
     * @param resultType the class of the result type
     * @param selectClause the select clause to apply
     * @param offset the offset to use
     * @param limit the limit to apply
     * @param <T> the type of result data
     * @return the list of data
     */
    public <T> List<T> fetchResults(Class<T> resultType, String selectClause, int offset, int limit) {
        return doFetchResults(resultType, selectClause, offset, limit, null, null);
    }

    /**
     * Fetch the list of data from the resultSet by applying the offset and limit.
     * @param selectClause the select clause to apply
     * @param offset the offset to use
     * @param limit the limit to apply
     * @param columnDataTypes data type to use per column
     * @param <T> the type of result data
     * @return the list of data
     */
    public <T> List<T> fetchResults(String selectClause, int offset, int limit, Map<Integer, Class<?>> columnDataTypes) {
        return doFetchResults(null, selectClause, offset, limit, columnDataTypes, null);
    }

    /**
     * Execute the delete query.
     * @return the updated row count
     */
    public int delete() {
        String queryString = "delete" + toString(getFromClause(), joinClause, whereClause);
        logger.log(Level.DEBUG, queryString);
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryString)) {
            setParameters(stmt);
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    /**
     * The string formatted as a sql query, by appending all values from this finder api.
     * @return a string formatted as a sql query
     */
    @Override
    public String toString() {
        return toString(getFromClause(), joinClause, whereClause, groupBy, orderBy);
    }

    /**
     * The Alias finder to select different data than the initial entity type used to create the finder.
     * @param <V> the type of value
     */
    public static final class AliasFinder<V> {
        private final Finder<?> finder;
        private final String alias;

        /**
         * Constructor with the finder and alias to use to create this instance.
         * @param finder the finder
         * @param alias the alias to fetch
         */
        public AliasFinder(Finder<?> finder, String alias) {
            this.finder = finder;
            this.alias = alias;
        }

        /**
         * Fetch the first row data from the resultSet.
         * @return the first instance if present, else null
         */
        public V fetchFirst() {
            return fetchFirst(null);
        }

        /**
         * Fetch the first row data from the resultSet using the field group.
         * @param fieldGroup the field group to select
         * @return the first instance if present, else null
         */
        public V fetchFirst(String fieldGroup) {
            List<V> resultDataList = finder.fetch(alias, fieldGroup, -1, 1, false);
            return resultDataList.isEmpty() ? null : resultDataList.get(0);
        }

        /**
         * Fetch the first row data from the resultSet for update.
         * @return the first instance if present, else null
         */
        public V fetchFirstForUpdate() {
            return fetchFirstForUpdate(null);
        }

        /**
         * Fetch the first row data from the resultSet using the field group, for update.
         * @param fieldGroup the field group to select
         * @return the first instance if present, else null
         */
        public V fetchFirstForUpdate(String fieldGroup) {
            List<V> resultDataList = finder.fetch(alias, fieldGroup, -1, 1, true);
            return resultDataList.isEmpty() ? null : resultDataList.get(0);
        }

        /**
         * Fetch the list of data from the resultSet.
         * @param firstResult the offset to use
         * @param maxResults the limit to apply
         * @return the list of data
         */
        public List<V> fetch(int firstResult, int maxResults) {
            return finder.fetch(alias, null, firstResult, maxResults, false);
        }

        /**
         * Fetch the list of data from the resultSet using the field group.
         * @param fieldGroup the field group to select
         * @param firstResult the offset to use
         * @param maxResults the limit to apply
         * @return the list of data
         */
        public List<V> fetch(String fieldGroup, int firstResult, int maxResults) {
            return finder.fetch(alias, fieldGroup, firstResult, maxResults, false);
        }

        /**
         * Fetch the list of data from the resultSet, for update.
         * @param firstResult the offset to use
         * @param maxResults the limit to apply
         * @return the list of data
         */
        public List<V> fetchForUpdate(int firstResult, int maxResults) {
            return finder.fetch(alias, null, firstResult, maxResults, true);
        }

        /**
         * Fetch the list of data from the resultSet using the field group, for update.
         * @param fieldGroup the field group to select
         * @param firstResult the offset to use
         * @param maxResults the limit to apply
         * @return the list of data
         */
        public List<V> fetchForUpdate(String fieldGroup, int firstResult, int maxResults) {
            return finder.fetch(alias, fieldGroup, firstResult, maxResults, true);
        }
    }
}
