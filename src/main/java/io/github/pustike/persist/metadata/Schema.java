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
package io.github.pustike.persist.metadata;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.github.pustike.persist.Column;
import io.github.pustike.persist.EntityListener;
import io.github.pustike.persist.Id;
import io.github.pustike.persist.Lob;
import io.github.pustike.persist.Table;
import io.github.pustike.persist.Version;
import io.github.pustike.persist.utils.PersistUtils;

/**
 * The entity schema metadata.
 */
public final class Schema {
    private final String name;
    private final EntityListener listener;
    private Map<String, EntityData> entityDataMap;

    private Schema(String name, EntityListener listener) {
        this.name = name;
        this.listener = listener;
        this.entityDataMap = new LinkedHashMap<>();
    }

    /**
     * Create the schema builder.
     * @return a new instance of schema builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get the schema name, can be {@code null} as it is optional.
     * @return the name of the schema
     */
    public String getName() {
        return name;
    }

    /**
     * Get the entity listener if configured, can be {@code null}.
     * @return the configured entity listener
     */
    public EntityListener getEntityListener() {
        return listener;
    }

    /**
     * Get the table name with schema for the given entity metadata.
     * @param entityData the entity data
     * @return the schemaName.tableName that can be used in sql query
     */
    public String toSchemaTableName(EntityData entityData) {
        String tableName = entityData.getTableName();
        return name == null ? tableName : name + '.' + tableName;
    }

    /**
     * Get the collection of all entity metadata configured in this schema.
     * @return entity metadata collection
     */
    public Collection<EntityData> getEntityData() {
        return Collections.unmodifiableCollection(entityDataMap.values());
    }

    /**
     * Get the entity metadata for the given class.
     * @param entityClass the entity class
     * @return the entity metadata
     */
    public EntityData getEntityData(Class<?> entityClass) {
        return getEntityData(entityClass.getName());
    }

    /**
     * Get the entity metadata for the given class name.
     * @param className the entity class name
     * @return the entity metadata
     */
    public EntityData getEntityData(String className) {
        EntityData entityData = entityDataMap.get(className);
        if (entityData == null) {
            throw new RuntimeException("Entity data is not available for class: " + className);
        }
        return entityData;
    }

    private void registerEntity(Class<?> entityClass) {
        EntityData entityData = createEntityData(entityClass);
        Class<?> superClass = entityClass.getSuperclass();
        EntityData currentEntityData = entityData;
        while (superClass != null && superClass != Object.class) {
            EntityData parentEntity = entityDataMap.get(superClass.getName());
            if (parentEntity == null) {
                parentEntity = createEntityData(superClass);
                entityDataMap.put(superClass.getName(), parentEntity);
            }
            currentEntityData.setParentEntity(parentEntity);
            currentEntityData = parentEntity;
            superClass = superClass.getSuperclass();
        }
        entityDataMap.put(entityClass.getName(), entityData);
    }

    private EntityData createEntityData(Class<?> entityClass) {
        String tableName = null;
        if (!Modifier.isAbstract(entityClass.getModifiers())) {
            Table table = entityClass.getDeclaredAnnotation(Table.class);
            if (table == null) {
                throw new IllegalStateException("@Table annotation must be specified for an Entity: " + entityClass);
            }
            tableName = PersistUtils.getSqlName(entityClass.getSimpleName(), table.name());
        }
        return new EntityData(entityClass, tableName);
    }

    private void configureMetadata() {
        for (EntityData entityData : entityDataMap.values()) {
            Class<?> entityClass = entityData.getEntityClass();
            for (Field field : entityClass.getDeclaredFields()) {
                addFieldData(entityData, field);
            }
            entityData.validate();
        }
    }

    private void addFieldData(EntityData entityData, Field field) {
        if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())
            || Modifier.isFinal(field.getModifiers())) {
            return;
        } // else, createFieldData by looking for @Column, @Id, @Version, @Lob etc.
        FieldData fieldData = new FieldData(field);
        Annotation[] annotations = field.getDeclaredAnnotations();
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(Column.class)) {
                fieldData.setColumn((Column) annotation);
            } else if (annotation.annotationType().equals(Id.class)) {
                fieldData.setColumnType(ColumnType.Id);
            } else if (annotation.annotationType().equals(Version.class)) {
                fieldData.setColumnType(ColumnType.Version);
                fieldData.setOptional(false);
            } else if (annotation.annotationType().equals(Lob.class)) {
                fieldData.setColumnType(ColumnType.Lob);
            }
        }
        // NOTE: should be called after @Column annotation has been read!
        fieldData.setColumnName(PersistUtils.getSqlName(field.getName(), fieldData.getColumnName()));
        if (entityDataMap.containsKey(field.getType().getName())) {
            fieldData.setColumnType(ColumnType.ForeignKey);
        }
        entityData.addField(fieldData);
    }

    /**
     * The schema metadata builder.
     */
    public static final class Builder {
        private String name;
        private Set<Class<?>> entitySet;
        private EntityListener listener;

        Builder() {
            this.entitySet = new LinkedHashSet<>();
        }

        /**
         * Set the name of the schema.
         * @param name the schema name
         * @return this builder instance
         */
        public Builder named(String name) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Invalid schema name: " + name);
            }
            this.name = name.toLowerCase(Locale.ROOT);
            return this;
        }

        /**
         * Add an entity class to the schema.
         * @param entityClass the entity class
         * @return this builder instance
         */
        public Builder add(Class<?> entityClass) {
            Objects.requireNonNull(entityClass);
            if (!entitySet.add(entityClass)) {
                throw new IllegalArgumentException("This entity is already added: " + entityClass);
            }
            return this;
        }

        /**
         * Set the entity listener which gets notified before insert or update.
         * @param listener the entity listener
         * @return this builder instance
         */
        public Builder having(EntityListener listener) {
            this.listener = Objects.requireNonNull(listener);
            return this;
        }

        /**
         * Build the fully configured schema instance.
         * @return the schema instance
         */
        public Schema build() {
            if (entitySet.isEmpty()) {
                throw new IllegalStateException("This schema doesn't contain any entity!");
            }
            Schema schema = new Schema(name, listener);
            for (Class<?> entityClass : entitySet) {
                schema.registerEntity(entityClass);
            }
            schema.configureMetadata();
            entitySet.clear();
            return schema;
        }
    }
}
