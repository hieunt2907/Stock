package com.hieunt.stock.query;

import org.springframework.data.jpa.domain.Specification;

import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLVisitor;

/**
 * RSQL Visitor chuyển đổi cây RSQL AST thành JPA Specification.
 * 
 * - AndNode (;) => Specification.and()
 * - OrNode (,) => Specification.or()
 * - ComparisonNode => GenericRsqlSpecification
 */
public class CustomRsqlVisitor<T> implements RSQLVisitor<Specification<T>, Void> {

    @Override
    public Specification<T> visit(AndNode node, Void param) {
        return node.getChildren().stream()
                .map(n -> n.accept(this, param))
                .reduce(Specification::and)
                .orElse(null);
    }

    @Override
    public Specification<T> visit(OrNode node, Void param) {
        return node.getChildren().stream()
                .map(n -> n.accept(this, param))
                .reduce(Specification::or)
                .orElse(null);
    }

    @Override
    public Specification<T> visit(ComparisonNode node, Void param) {
        return new GenericRsqlSpecification<>(
                node.getSelector(),
                node.getOperator(),
                node.getArguments());
    }
}
