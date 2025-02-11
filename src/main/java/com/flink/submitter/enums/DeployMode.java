package com.flink.submitter.enums;

import com.flink.submitter.client.CommonClient;
import com.flink.submitter.client.SessionClient;
import com.flink.submitter.client.YarnApplicationClient;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author daitf
 * @date 2025/1/15
 */
@AllArgsConstructor
@Getter
public enum DeployMode {
    SESSION(new SessionClient()),

    YARN_APPLICATION(new YarnApplicationClient()),

    YARN_PRE_JOB(null),

    ;

    private final CommonClient client;
}
