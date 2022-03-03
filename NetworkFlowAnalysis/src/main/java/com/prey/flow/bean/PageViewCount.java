package com.prey.flow.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;
}
