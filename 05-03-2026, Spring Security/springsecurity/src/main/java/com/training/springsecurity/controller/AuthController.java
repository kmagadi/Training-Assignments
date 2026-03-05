package com.training.springsecurity.controller;

import com.training.springsecurity.dto.AuthResponse;
import com.training.springsecurity.dto.LoginRequest;
import com.training.springsecurity.dto.RegisterRequest;
import com.training.springsecurity.model.User;
import com.training.springsecurity.service.AuthService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/auth")
public class AuthController
{
    private final AuthService authService;

    public AuthController(AuthService authService)
    {
        this.authService = authService;
    }

    @PostMapping("/register")
    public Mono<User> register(@RequestBody RegisterRequest request)
    {
        return authService.register(request);
    }

    @PostMapping("/login")
    public Mono<AuthResponse> login(@RequestBody LoginRequest request)
    {
        return authService.login(request);
    }
}