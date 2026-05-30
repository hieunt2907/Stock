package com.hieunt.stock.service;

import java.util.List;

import com.hieunt.stock.model.response.CompanyResponse;

public interface CompanyService {
    List<CompanyResponse> findCompanies();

    CompanyResponse findCompany(String symbol);
}
