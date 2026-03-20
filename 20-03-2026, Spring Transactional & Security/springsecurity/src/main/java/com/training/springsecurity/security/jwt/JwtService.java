package com.training.springsecurity.security.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class JwtService {

    private final String secret;

    // 1 hour expiry
    private static final long EXPIRATION = 1000 * 60 * 60;

    public JwtService(String secret) {
        this.secret = secret;
    }

    // 🔥 Proper secure key (Base64 decoded)
    private Key getKey() {
        byte[] keyBytes = Decoders.BASE64.decode(secret);
        return Keys.hmacShaKeyFor(keyBytes);
    }

    // 🔥 Generate token with roles
    public String generateToken(Authentication auth) {

        List<String> roles = extractRoles(auth.getAuthorities());

        return Jwts.builder()
                .setSubject(auth.getName())
                .claim("roles", roles)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + EXPIRATION))
                .signWith(getKey())
                .compact();
    }

    // 🔥 Extract username
    public String extractUsername(String token) {
        return extractAllClaims(token).getSubject();
    }

    // 🔥 Extract roles
    public List<String> extractRoles(String token) {
        return extractAllClaims(token).get("roles", List.class);
    }

    // 🔥 Validate token
    public boolean isTokenValid(String token, String username) {
        String extractedUsername = extractUsername(token);
        return extractedUsername.equals(username) && !isTokenExpired(token);
    }

    // 🔥 Check expiry
    private boolean isTokenExpired(String token) {
        return extractAllClaims(token)
                .getExpiration()
                .before(new Date());
    }

    // 🔥 Central claims parser
    private Claims extractAllClaims(String token) {
        return Jwts.parserBuilder()
                .setSigningKey(getKey())
                .build()
                .parseClaimsJws(token)
                .getBody();
    }

    // 🔥 Utility to extract roles from Authentication
    private List<String> extractRoles(Collection<? extends GrantedAuthority> authorities) {
        return authorities.stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.toList());
    }
}