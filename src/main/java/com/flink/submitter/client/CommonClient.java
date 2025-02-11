package com.flink.submitter.client;

import com.flink.submitter.bean.FlinkJobArgs;
import com.flink.submitter.bean.FlinkJobStatus;

import java.util.List;

/**
 * @author daitf
 * @date 2025/1/15
 */
public interface CommonClient {
    /**
     * submit a flink job
     *
     * @param task the flink job args
     * @return the job id
     */
    String submit(FlinkJobArgs task);

    /**
     * cancel a flink job
     *
     * @param jobId the job id
     * @return cancel result
     */
    Boolean cancel(String jobId);

    /**
     * get the status of all flink job
     *
     * @return the list of flink job status
     */
    List<FlinkJobStatus> list();
}
