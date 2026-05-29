package com.hieunt.stock.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.BaseResponse;

@RestControllerAdvice
public class AdviceController {

    private final static Logger log = LoggerFactory.getLogger(AdviceController.class);

    @ExceptionHandler(HIEUNTException.class)
    public ResponseEntity<BaseResponse<Void>> handleHIEUNTException(HIEUNTException ex) {
        log.info("handleHIEUNTException: {}", ex.getMessage());

        BaseResponse<Void> response = new BaseResponse<>();
        response.setCode(ex.getHttpStatus());   // code business
        response.setMessage(ex.getMessage());

    return ResponseEntity
            .status(ex.getHttpStatus())     // ❗ QUAN TRỌNG
            .body(response);
}


}