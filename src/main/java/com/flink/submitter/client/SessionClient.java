package com.flink.submitter.client;

import com.flink.submitter.bean.FlinkJobArgs;
import com.flink.submitter.bean.FlinkJobStatus;
import com.flink.submitter.config.SessionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.util.RestClientException;

import java.io.File;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * @author daitf
 * @date 2025/1/17
 */
@Slf4j
public class SessionClient implements CommonClient {

    @Override
    public String submit(FlinkJobArgs task) {
        Configuration configuration = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance())) {
            // dependency jars
            List<URL> urls = new ArrayList<>();
            File libDir = new File(SessionConfig.FLINK_LIB_JAR_PATH);
            if (libDir.exists() && libDir.isDirectory()) {
                File[] files = Optional.ofNullable(libDir.listFiles()).orElse(new File[0]);
                for (File file : files) {
                    URL fileUrl = file.toURI().toURL();
                    urls.add(fileUrl);
                }
            }
            log.info("dependency urls: {}", urls);
            File taskJar = new File(SessionConfig.FLINK_LIB_JAR_PATH);

            PackagedProgram program = PackagedProgram
                    .newBuilder()
                    .setJarFile(taskJar)
                    .setConfiguration(configuration)
                    .setEntryPointClassName(task.getEntryPointClassName())
                    .setArguments(task.getArgs())
                    .setUserClassPaths(urls)
                    .build();
            //创建任务
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, 1, true);
            //提交任务
            JobID jobId = client.submitJob(jobGraph).get();
            return jobId.toString();
        } catch (Exception e) {
            throw new RuntimeException("提交任务失败", e);
        }
    }

    @Override
    public Boolean cancel(String jobId) {
        Configuration configuration = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());) {
            JobStatus jobStatus = client.getJobStatus(JobID.fromHexString(jobId)).get();
            if (jobStatus == null || jobStatus.equals(JobStatus.FAILED) ||
                    jobStatus.equals(JobStatus.CANCELED) || jobStatus.equals(JobStatus.FINISHED)) {
                return true;
            }
            client.cancel(JobID.fromHexString(jobId)).get();
            return true;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            // todo handle not found job exception
            if (cause instanceof RestClientException && cause.getMessage().contains("Job not found")) {
                return true;
            }
            log.error("取消任务失败", e);
        }
        return false;
    }

    @Override
    public List<FlinkJobStatus> list() {
        List<FlinkJobStatus> result = new ArrayList<>();
        Configuration configuration = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());) {
            Collection<JobStatusMessage> jobStatusMessages = client.listJobs().get();
            for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
                result.add(
                        FlinkJobStatus.builder()
                                .id(jobStatusMessage.getJobId().toString())
                                .name(jobStatusMessage.getJobName())
                                .status(jobStatusMessage.getJobState().name())
                                .startTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(jobStatusMessage.getStartTime()), ZoneId.systemDefault()))
                                .build()
                );
            }
        } catch (Exception e) {
            log.error("获取任务列表失败", e);
        }

        return result;
    }

    private Configuration getConfiguration() {
        String[] address = SessionConfig.JOB_MANAGER_ADDRESS.replace("http://", "").split(":");
        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, address[0]);
        configuration.setInteger(JobManagerOptions.PORT, 6123);
        configuration.setInteger(RestOptions.PORT, Integer.parseInt(address[1]));
        configuration.setString(DeploymentOptions.TARGET, "remote");
        return configuration;
    }
}
