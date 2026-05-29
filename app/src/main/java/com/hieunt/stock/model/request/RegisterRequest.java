package com.hieunt.stock.model.request;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

import lombok.Data;

@Data
public class RegisterRequest {
    @Email(message = "Email khong hop le")
    @NotBlank(message = "Email khong duoc de trong")
    private String email;

    @NotBlank(message = "Password khong duoc de trong")
    @Size(min = 6, message = "Password phai it nhat 6 ky tu")
    private String password;
}
