package com.prey.market.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ChannelPromotionCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;

}
