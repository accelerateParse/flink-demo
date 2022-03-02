package com.prey.market.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AdCountViewByProvince {
    private String province;
    private String windowEnd;
    private Long count;
}
