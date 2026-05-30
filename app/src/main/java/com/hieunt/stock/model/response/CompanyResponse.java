package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CompanyResponse {
    private String symbol;
    private String companyName;
    private String exchange;
    private String industry;
    private String sector;
    private String country;
    private String currency;
    private String website;
    private String description;
    private BigDecimal marketCap;
    private BigDecimal sharesOutstanding;
    private String source;
    private LocalDateTime updatedAt;
}
