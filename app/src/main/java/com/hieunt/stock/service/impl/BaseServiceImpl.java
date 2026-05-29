package com.hieunt.stock.service.impl;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.HttpStatus;

import com.hieunt.stock.constant.Status;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.mapper.BaseMapper;
import com.hieunt.stock.query.CustomRsqlVisitor;
import com.hieunt.stock.repository.BaseRepository;
import com.hieunt.stock.repository.entity.BaseEntity;
import com.hieunt.stock.service.BaseService;

import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;

public abstract class BaseServiceImpl<E extends BaseEntity> implements BaseService<E> {
    protected abstract BaseRepository<E> getBaseRepository();

    protected abstract BaseMapper<E> getBaseMapper();

    @Override
    public E create(E e) throws HIEUNTException {
        E entity = getBaseMapper().toEntity(e);
        entity.setStatus(Status.ACTIVE);
        return getBaseRepository().save(entity);
    }

    @Override
    public void update(Long id, E e) throws HIEUNTException {
        E entity = getBaseMapper().updateEntity(e, findById(id));
        getBaseRepository().save(entity);
    }

    @Override
    public void delete(Long id) throws HIEUNTException {
        E entity = findById(id);
        entity.setStatus(Status.DELETED);
        entity.setDeletedAt(OffsetDateTime.now());
        entity.setDeletedBy(com.hieunt.stock.util.SecurityUtil.getCurrentUsername().orElse(null));
        getBaseRepository().save(entity);
    }

    @Override
    public E findById(Long id) throws HIEUNTException {
        return getBaseRepository().findById(id)
                .orElseThrow(() -> new HIEUNTException(HttpStatus.NOT_FOUND, "Not found", "404"));
    }

    @Override
    public List<E> findAll() throws HIEUNTException {
        return getBaseRepository().findAll();
    }

    @Override
    public Page<E> search(String filter, Pageable pageable) throws HIEUNTException {
        if (filter == null || filter.isBlank()) {
            return getBaseRepository().findAll(pageable);
        }

        Node rootNode = new RSQLParser().parse(filter);
        Specification<E> spec = rootNode.accept(new CustomRsqlVisitor<>());
        return getBaseRepository().findAll(spec, pageable);
    }

}
