# Spring Boot Role-Based API with JWT Authentication

## Overview

This project demonstrates a simple Spring Boot REST API secured using Spring Security and JSON Web Tokens (JWT). The application implements authentication and role-based authorization for users with different roles such as USER and ADMIN.

The system allows users to register and log in. After successful authentication, a JWT token is generated and must be included in the Authorization header for accessing protected APIs.

The project uses an H2 in-memory database for persistence and Spring Data JPA for data access.

---

## Tech Stack

* Java 21
* Spring Boot 3.5
* Spring Security
* Spring Data JPA
* JWT (jjwt library)
* H2 Database
* Maven
* Lombok

---

## Project Structure

```
com.training.springtraining

config
    JwtAuthenticationFilter.java
    SecurityConfig.java

controller
    AuthController.java
    DemoController.java

dto
    AuthRequest.java
    AuthResponse.java
    RegisterRequest.java

entity
    Role.java
    User.java

jwt
    JwtService.java

repository
    UserRepository.java

service
    AuthService.java
```

---

## Security Architecture

The application implements JWT-based authentication and role-based authorization using Spring Security.

Authentication Flow:

1. A user registers using the `/auth/register` endpoint.
2. The user's password is encrypted and stored in the database.
3. The user logs in using `/auth/login`.
4. If credentials are valid, a JWT token is generated.
5. The token must be sent in the `Authorization` header when accessing protected APIs.
6. A custom JWT filter intercepts incoming requests and validates the token.
7. If the token is valid, the user is authenticated and allowed to access secured endpoints based on their role.

---

## Roles

The system supports two roles:

* USER
* ADMIN

Authorization rules:

* `/api/user` can be accessed by USER and ADMIN
* `/api/admin` can only be accessed by ADMIN

---

## API Endpoints

### Register User

```
POST /auth/register
```

Request Body

```
{
  "username": "john",
  "password": "1234",
  "role": "USER"
}
```

Response

```
User registered
```

---

### Login

```
POST /auth/login
```

Request Body

```
{
  "username": "john",
  "password": "1234"
}
```

Response

```
{
  "token": "jwt_token_value"
}
```

---

### Access User API

```
GET /api/user
```

Header

```
Authorization: Bearer <jwt_token>
```

Response

```
Hello User
```

Accessible by roles:

* USER
* ADMIN

---

### Access Admin API

```
GET /api/admin
```

Header

```
Authorization: Bearer <jwt_token>
```

Response

```
Hello Admin
```

Accessible by roles:

* ADMIN only

---

## Database Configuration

The application uses the H2 in-memory database.

Configuration:

```
spring.datasource.url=jdbc:h2:mem:testdb
spring.datasource.driverClassName=org.h2.Driver
spring.jpa.hibernate.ddl-auto=update
spring.h2.console.enabled=true
```

H2 Console can be accessed at:

```
http://localhost:8080/h2-console
```

---

## Running the Application

1. Clone the repository.

2. Build the project using Maven.

```
mvn clean install
```

3. Run the application.

```
mvn spring-boot:run
```

The application will start on:

```
http://localhost:8080
```

---

## Testing with Postman

Step 1: Register a user using `/auth/register`.

Step 2: Login using `/auth/login` to obtain the JWT token.

Step 3: Include the token in the Authorization header when calling protected APIs.

Example header:

```
Authorization: Bearer <your_token_here>
```

---

## Key Features

* Spring Security integration
* JWT based authentication
* Role based authorization
* Secure REST APIs
* H2 in-memory database
* Clean layered architecture
* Password encryption using BCrypt
