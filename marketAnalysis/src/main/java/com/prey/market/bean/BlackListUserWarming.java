package com.prey.market.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BlackListUserWarming {
    private Long userId;
    private Long adId;
    private String warmingMsg;


}
