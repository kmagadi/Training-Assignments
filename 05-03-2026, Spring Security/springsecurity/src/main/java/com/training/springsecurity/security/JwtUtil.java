package com.training.springsecurity.security;

import com.training.springsecurity.model.User;
import org.springframework.stereotype.Component;

@Component
public class JwtUtil
{
    public String generateToken(User user)
    {
        // In a real application, you would use a library like jjwt to generate a JWT token
        // Here we will just return a dummy token for demonstration purposes
        return "dummy-jwt-token-for-user-" + user.getUsername();
    }

    public String generateToken(String username, String role) {
        // In a real application, you would use a library like jjwt to generate a JWT token
        // Here we will just return a dummy token for demonstration purposes
        return "dummy-jwt-token-for-user-" + username + "-with-role-" + role;
    }
}
