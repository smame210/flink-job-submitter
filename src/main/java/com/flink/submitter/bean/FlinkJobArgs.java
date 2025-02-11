package com.flink.submitter.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author daitf
 * @date 2025/1/13
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FlinkJobArgs {
    private String jobName;

    @Builder.Default
    private String entryPointClassName = "cn.cicv.cloud.flink.Main";

    private String args;
}
