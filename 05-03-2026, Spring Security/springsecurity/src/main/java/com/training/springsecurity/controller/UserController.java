package com.training.springsecurity.controller;

import com.training.springsecurity.dto.ProfileResponse;
import com.training.springsecurity.model.User;
import com.training.springsecurity.repository.UserRepository;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api")
public class UserController
{
    private final UserRepository userRepository;

    public UserController(UserRepository userRepository)
    {
        this.userRepository = userRepository;
    }

    @GetMapping("/user/profile")
    public Mono< ProfileResponse > getProfile(Authentication authentication)
    {
        return userRepository.findByUsername(authentication.getName())
                .map(user ->
                        new ProfileResponse(
                                user.getUsername(),
                                user.getRole()
                        )
                );
    }

    @GetMapping("/admin/users")
    public Flux<User> getAllUsers()
    {
        return userRepository.findAll();
    }
}