package com.prey.market.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
