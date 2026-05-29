package com.hieunt.stock.model.request;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

import lombok.Data;

@Data
public class RoleRequest {
    @NotBlank(message = "Role name khong duoc de trong")
    @Size(max = 50, message = "Role name toi da 50 ky tu")
    private String name;
}
