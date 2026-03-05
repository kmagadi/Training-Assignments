package com.training.springsecurity.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("users")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User
{
    @Id
    private Long id;
    @Column("username")
    private String username;
    @Column("password")
    private String password;
    @Column("role")
    private String role;
}
