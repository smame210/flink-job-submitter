package com.flink.submitter;

import com.flink.submitter.bean.FlinkJobArgs;
import com.flink.submitter.client.CommonClient;
import com.flink.submitter.enums.DeployMode;
import lombok.extern.slf4j.Slf4j;

/**
 * @author daitf
 * @date 2025/1/13
 */
@Slf4j
public class JobStarter {
    // 任务名称
    private static final String TASK_NAME = "word_count_task";

    // 任务参数
    private static final String TASK_ARGS = "";

    public static void main(String[] args) {
        // 构造任务参数
        FlinkJobArgs flinkJobArgs = FlinkJobArgs.builder()
                .jobName(TASK_NAME)
                .entryPointClassName("org.apache.flink.streaming.examples.wordcount.WordCount")
                .args(TASK_ARGS)
                .build();
        log.info("flink job args: {}", flinkJobArgs);

        // 指定部署模式
        DeployMode mode = DeployMode.YARN_APPLICATION;
        CommonClient client;
        switch (mode) {
            case SESSION:
                client = DeployMode.SESSION.getClient();
                String sessionSubmit = client.submit(flinkJobArgs);
                log.info("submit result: {}", sessionSubmit);
                break;
            case YARN_PRE_JOB:
                // todo do something for per-job mode
                break;
            case YARN_APPLICATION:
                client = DeployMode.YARN_APPLICATION.getClient();
                String yarnApplicationSubmit = client.submit(flinkJobArgs);
                log.info("submit result: {}", yarnApplicationSubmit);
                break;
            default:
                throw new IllegalArgumentException("Unsupported deploy mode: " + mode);
        }
    }
}
