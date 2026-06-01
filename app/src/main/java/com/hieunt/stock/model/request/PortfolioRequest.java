package com.hieunt.stock.model.request;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

import lombok.Data;

@Data
public class PortfolioRequest {
    @NotBlank(message = "Portfolio name khong duoc de trong")
    @Size(max = 100, message = "Portfolio name toi da 100 ky tu")
    private String name;
}
