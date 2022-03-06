package com.prey.login.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LoginFailWarming {
    private Long userId;
    private String fistFailTime;
    private String lastFailTime;
    private String msg;
}
