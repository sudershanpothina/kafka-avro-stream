package com.spothi.provi;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Provider {
    @JsonProperty("ID")
    private String id;
    @JsonProperty("NAME")
    private String name;
//    @JsonProperty("SOURCE_SYSTEM_DTTM")
//    private String sourceSystemDttm;
}
