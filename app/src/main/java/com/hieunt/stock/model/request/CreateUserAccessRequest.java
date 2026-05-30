package com.hieunt.stock.model.request;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

import lombok.Data;

@Data
public class CreateUserAccessRequest {
    @Email(message = "Email khong hop le")
    @NotBlank(message = "Email khong duoc de trong")
    private String email;

    @NotBlank(message = "Password khong duoc de trong")
    @Size(min = 6, message = "Password phai it nhat 6 ky tu")
    private String password;

    private List<String> roles = new ArrayList<>();

    private List<String> permissions = new ArrayList<>();
}
