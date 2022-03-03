package com.prey.flow.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LogEvent {
    private String ip;
    private String userId;
    private Long timestamp;
    private String method;
    private String url;
}
