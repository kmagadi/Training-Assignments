package com.training.springsecurity.dto;

import lombok.*;

@Getter
@Setter
public class LoginRequest {
    private String username;
    private String password;
}