package com.hieunt.stock.model.request;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

import lombok.Data;

@Data
public class VerifyOtpRequest {
    @Email(message = "Email khong hop le")
    @NotBlank(message = "Email khong duoc de trong")
    private String email;

    @NotBlank(message = "OTP khong duoc de trong")
    @Pattern(regexp = "\\d{6}", message = "OTP phai gom 6 chu so")
    private String otp;
}
