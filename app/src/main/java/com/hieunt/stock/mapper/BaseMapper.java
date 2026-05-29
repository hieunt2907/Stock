package com.hieunt.stock.mapper;

import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

public interface BaseMapper<E> {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    @Mapping(target = "createdBy", ignore = true)
    @Mapping(target = "updatedBy", ignore = true)
    @Mapping(target = "deletedAt", ignore = true)
    E toEntity(E e);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    @Mapping(target = "createdBy", ignore = true)
    @Mapping(target = "updatedBy", ignore = true)
    @Mapping(target = "deletedAt", ignore = true)
    E updateEntity(E e, @MappingTarget E entity);

    // @AfterMapping
    // default void upperAllString(@MappingTarget E entity) {
    // Class<?> clazz = entity.getClass();
    // while (clazz != null) {
    // Field[] fields = clazz.getDeclaredFields();
    // for (Field field : fields) {
    // if (field.getType().equals(String.class)) {
    // field.setAccessible(true);
    // try {
    // String value = (String) field.get(entity);
    // if (value != null) {
    // field.set(entity, value.toUpperCase());
    // }
    // } catch (IllegalAccessException ignored) {
    // }
    // }
    // }
    // clazz = clazz.getSuperclass();
    // }
    // }
}
