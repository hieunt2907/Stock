package com.hieunt.stock.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.BaseResponse;

@RestControllerAdvice
public class AdviceController {

    private static final Logger log = LoggerFactory.getLogger(AdviceController.class);

    @ExceptionHandler(HIEUNTException.class)
    public ResponseEntity<BaseResponse<Void>> handleHIEUNTException(HIEUNTException ex) {
        log.info("handleHIEUNTException: {}", ex.getMessage());

        BaseResponse<Void> response = new BaseResponse<>();
        response.setCode(ex.getHttpStatus());
        response.setMessage(ex.getMessage());

        return ResponseEntity
                .status(ex.getHttpStatus())
                .body(response);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<BaseResponse<Void>> handleAccessDeniedException(AccessDeniedException ex) {
        log.info("handleAccessDeniedException: {}", ex.getMessage());

        BaseResponse<Void> response = new BaseResponse<>();
        response.setCode(HttpStatus.FORBIDDEN);
        response.setMessage(ex.getMessage());

        return ResponseEntity
                .status(HttpStatus.FORBIDDEN)
                .body(response);
    }
}
