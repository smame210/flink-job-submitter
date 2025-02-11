package com.flink.submitter.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @author daitf
 * @date 2025/1/15
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FlinkJobStatus {
    private String id;

    private String name;

    private String status;

    private LocalDateTime startTime;
}
