package com.hieunt.stock.service;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.hieunt.stock.exception.HIEUNTException;

public interface BaseService<T> {
    T create(T t) throws HIEUNTException;

    void update(Long id, T t) throws HIEUNTException;

    void delete(Long id) throws HIEUNTException;

    T findById(Long id) throws HIEUNTException;

    Page<T> search(String search, Pageable pageable) throws HIEUNTException;

    List<T> findAll() throws HIEUNTException;

}
