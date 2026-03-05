package com.training.springsecurity.service;

import com.training.springsecurity.dto.AuthResponse;
import com.training.springsecurity.dto.LoginRequest;
import com.training.springsecurity.dto.RegisterRequest;
import com.training.springsecurity.model.User;
import com.training.springsecurity.repository.UserRepository;
import com.training.springsecurity.security.JwtUtil;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class AuthService
{
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;

    public AuthService(UserRepository userRepository,
                       PasswordEncoder passwordEncoder,
                       JwtUtil jwtUtil)
    {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtUtil = jwtUtil;
    }

    public Mono<User> register(RegisterRequest request)
    {
        return userRepository.findByUsername(request.getUsername())
                .flatMap(existingUser ->
                        Mono.<User>error(new RuntimeException("Username already exists"))
                )
                .switchIfEmpty(
                        Mono.defer(() -> {

                            User user = User.builder()
                                    .username(request.getUsername())
                                    .password(passwordEncoder.encode(request.getPassword()))
                                    .role(request.getRole())
                                    .build();

                            return userRepository.save(user);
                        })
                );
    }

    public Mono<AuthResponse> login(LoginRequest request)
    {
        return userRepository.findByUsername(request.getUsername())
                .filter(user ->
                        passwordEncoder.matches(
                                request.getPassword(),
                                user.getPassword()
                        )
                )
                .map(user -> {

                    String token = jwtUtil.generateToken(user);

                    return new AuthResponse(token);
                })
                .switchIfEmpty(
                        Mono.error(
                                new RuntimeException("Invalid username or password")
                        )
                );
    }
}