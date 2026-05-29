package com.hieunt.stock.exception;

import org.springframework.http.HttpStatus;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@RequiredArgsConstructor
public class HIEUNTException extends Exception {
    private final HttpStatus httpStatus;
    private final String messageCode;
    private final String errCode;

    public static HIEUNTException getByMessageAndCode(String messageCode, String errCode) {
        return HIEUNTException.builder()
                .messageCode(messageCode)
                .errCode(errCode)
                .build();
    }

    @Override
    public String getMessage() {
        return String.format("%s: %s", messageCode, errCode);
    }
}
