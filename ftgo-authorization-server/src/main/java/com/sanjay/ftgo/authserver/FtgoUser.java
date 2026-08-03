package com.sanjay.ftgo.authserver;

public record FtgoUser(long id, String username, String password, String role) {
}
