package com.hieunt.stock.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;

import java.util.Collection;

@Getter
@Setter
public class CustomUserDetails extends User {
    private Long id;
    public CustomUserDetails(String username, String password, Collection<? extends GrantedAuthority> authorities,
            Long id) {
        super(username, password, authorities);
        this.id = id;
    }
}
