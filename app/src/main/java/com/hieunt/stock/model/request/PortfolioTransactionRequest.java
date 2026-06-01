package com.hieunt.stock.model.request;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import lombok.Data;

@Data
public class PortfolioTransactionRequest {
    @NotBlank(message = "Symbol khong duoc de trong")
    private String symbol;

    @Pattern(regexp = "BUY|SELL", message = "Transaction type phai la BUY hoac SELL")
    private String transactionType;

    @NotNull(message = "Quantity khong duoc de trong")
    @DecimalMin(value = "0.0001", message = "Quantity phai lon hon 0")
    private BigDecimal quantity;

    @NotNull(message = "Price khong duoc de trong")
    @DecimalMin(value = "0.0001", message = "Price phai lon hon 0")
    private BigDecimal price;

    private LocalDateTime transactionTime;
}
