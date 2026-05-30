package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.response.CompanyResponse;
import com.hieunt.stock.service.CompanyService;
import com.hieunt.stock.util.SecurityUtil;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/companies")
@RequiredArgsConstructor
public class CompanyController {

    private final CompanyService companyService;

    @GetMapping
    public ResponseEntity<BaseResponse<List<CompanyResponse>>> findCompanies() {
        checkAccess("read");
        return ResponseEntity.ok(success(companyService.findCompanies()));
    }

    @GetMapping("/{symbol}")
    public ResponseEntity<BaseResponse<CompanyResponse>> findCompany(@PathVariable String symbol) {
        checkAccess("read");
        return ResponseEntity.ok(success(companyService.findCompany(symbol)));
    }

    private void checkAccess(String action) {
        SecurityUtil.checkAnyRole(List.of("ADMIN", "ANALYST", "USER"));
        SecurityUtil.checkPermission("company", action);
    }

    private <T> BaseResponse<T> success(T data) {
        return BaseResponse.<T>builder()
                .code(HttpStatus.OK)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
