package com.hieunt.stock.query;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.data.jpa.domain.Specification;

import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.RSQLOperators;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class GenericRsqlSpecification<T> implements Specification<T> {

    private String property;
    private ComparisonOperator operator;
    private List<String> arguments;

    @Override
    public Predicate toPredicate(Root<T> root, CriteriaQuery<?> query, CriteriaBuilder builder) {
        Path<String> propertyPath = parseProperty(root, property);
        List<Object> args = castArguments(propertyPath);
        Object argument = args.get(0);

        if (operator.equals(RSQLOperators.EQUAL)) {
            if (argument instanceof String) {
                return builder.like(builder.lower(propertyPath),
                        "%" + argument.toString().toLowerCase() + "%");
            } else {
                return builder.equal(propertyPath, argument);
            }
        } else if (operator.equals(RSQLOperators.NOT_EQUAL)) {
            if (argument instanceof String) {
                return builder.notLike(builder.lower(propertyPath),
                        "%" + argument.toString().toLowerCase() + "%");
            } else {
                return builder.notEqual(propertyPath, argument);
            }
        } else if (operator.equals(RSQLOperators.GREATER_THAN)) {
            return builder.greaterThan(propertyPath, argument.toString());
        } else if (operator.equals(RSQLOperators.GREATER_THAN_OR_EQUAL)) {
            return builder.greaterThanOrEqualTo(propertyPath, argument.toString());
        } else if (operator.equals(RSQLOperators.LESS_THAN)) {
            return builder.lessThan(propertyPath, argument.toString());
        } else if (operator.equals(RSQLOperators.LESS_THAN_OR_EQUAL)) {
            return builder.lessThanOrEqualTo(propertyPath, argument.toString());
        } else if (operator.equals(RSQLOperators.IN)) {
            return propertyPath.in(args);
        } else if (operator.equals(RSQLOperators.NOT_IN)) {
            return builder.not(propertyPath.in(args));
        }

        return null;
    }

    /**
     * Hỗ trợ truy xuất thuộc tính lồng nhau (nested property).
     * Ví dụ: "spec.name" sẽ tạo ra root.get("spec").get("name")
     */
    private Path<String> parseProperty(Root<T> root, String property) {
        if (property.contains(".")) {
            String[] parts = property.split("\\.");
            Path<String> path = root.get(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                path = path.get(parts[i]);
            }
            return path;
        }
        return root.get(property);
    }

    /**
     * Ép kiểu arguments theo kiểu dữ liệu thực tế của property trong entity.
     */
    private List<Object> castArguments(Path<?> propertyPath) {
        Class<?> type = propertyPath.getJavaType();

        return arguments.stream().map(arg -> {
            if (type.equals(Integer.class) || type.equals(int.class)) {
                return Integer.parseInt(arg);
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                return Long.parseLong(arg);
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                return Double.parseDouble(arg);
            } else if (type.equals(Float.class) || type.equals(float.class)) {
                return Float.parseFloat(arg);
            } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
                return Boolean.parseBoolean(arg);
            } else if (type.equals(OffsetDateTime.class)) {
                return OffsetDateTime.parse(arg);
            } else {
                return arg;
            }
        }).collect(Collectors.toList());
    }
}
