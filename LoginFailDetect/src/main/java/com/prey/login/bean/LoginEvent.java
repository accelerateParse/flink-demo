package com.prey.login.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String loginState;
    private Long timestamp;
}
