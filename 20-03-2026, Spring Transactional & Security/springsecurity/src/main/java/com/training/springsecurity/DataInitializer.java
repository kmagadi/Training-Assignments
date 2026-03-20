package com.training.springsecurity;

import com.training.springsecurity.entity.UserEntity;
import com.training.springsecurity.repository.UserRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class DataInitializer implements CommandLineRunner {

    private final UserRepository repo;
    private final PasswordEncoder encoder;

    public DataInitializer(UserRepository repo, PasswordEncoder encoder) {
        this.repo = repo;
        this.encoder = encoder;
    }

    @Override
    public void run(String... args) {

        if (repo.findByUsername("user").isEmpty()) {
            UserEntity user = new UserEntity();
            user.setUsername("user");
            user.setPassword(encoder.encode("password"));
            user.setRole("USER");
            repo.save(user);
        }

        if (repo.findByUsername("admin").isEmpty()) {
            UserEntity admin = new UserEntity();
            admin.setUsername("admin");
            admin.setPassword(encoder.encode("password"));
            admin.setRole("ADMIN");
            repo.save(admin);
        }
    }
}