package com.hieunt.stock.model.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AuthFlowResponse {
    private String email;

    @JsonProperty("registered")
    private boolean registered;

    @JsonProperty("otp_sent")
    private boolean otpSent;

    @JsonProperty("next_step")
    private String nextStep;

    @JsonProperty("expires_in")
    private Long expiresIn;
}
