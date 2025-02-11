package com.flink.submitter.client;

import com.flink.submitter.bean.CustomOptions;
import com.flink.submitter.bean.FlinkJobArgs;
import com.flink.submitter.bean.FlinkJobStatus;
import com.flink.submitter.config.EnvConfig;
import com.flink.submitter.config.YarnApplicationConfig;
import com.flink.submitter.util.YarnUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;
import static org.apache.flink.yarn.configuration.YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME;

/**
 * @author daitf
 * @date 2025/1/15
 */
@Slf4j
public class YarnApplicationClient implements CommonClient {


    @Override
    public String submit(FlinkJobArgs task) {
        // load flink configuration
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(getFlinkConfDir());
        flinkConfig

                .set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName())
                // job name
                .set(YarnConfigOptions.APPLICATION_NAME, task.getJobName())
                .set(CustomOptions.ENV_JAVA_OPTS, "-Dflink_job_name=" + task.getJobName() + " -DyarnContainerId=$CONTAINER_ID")
                //flink on yarn dependency
                .set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(new Path(YarnApplicationConfig.FLINK_LIB_JAR_PATH).toString()))
                .set(YarnConfigOptions.FLINK_DIST_JAR, YarnApplicationConfig.FLINK_DIST_JAR_PATH)
                .set(PipelineOptions.JARS, Collections.singletonList(new Path(YarnApplicationConfig.TASK_JAR_PATH).toString()))
                .set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, new File(getFlinkConfDir(), CONFIG_FILE_LOG4J_NAME).getPath())
                // other options
                .set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES))
                .set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));

        DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
        ClusterClientFactory<ApplicationId> clientFactory = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);
        ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(flinkConfig);

        String[] programArguments = Optional.ofNullable(task.getArgs()).orElse("").trim().split("\\s+");
        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(programArguments, task.getEntryPointClassName());

        ClusterClient<ApplicationId> clusterClient = null;
        YarnClient yarnClient = YarnUtil.getYarnClient();
        YarnConfiguration yarnConf = (YarnConfiguration) yarnClient.getConfig();
        try (YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                flinkConfig,
                yarnConf,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);) {

            ClusterClientProvider<ApplicationId> clusterClientProvider = clusterDescriptor.deployApplicationCluster(
                    clusterSpecification,
                    applicationConfiguration);

            clusterClient = clusterClientProvider.getClusterClient();
            ApplicationId applicationId = clusterClient.getClusterId();
            String webInterfaceURL = clusterClient.getWebInterfaceURL();
            log.info("\n" +
                    "|-------------------------job start success------------------------|\n" +
                    "|Flink Job Started: applicationId: " + applicationId + "  |\n" +
                    "|Flink Job Web Url: " + webInterfaceURL + "                    |\n" +
                    "|__________________________________________________________________|");
            return applicationId.toString();
        } catch (ClusterDeploymentException e) {
            throw new RuntimeException(e);
        } finally {
            if (clusterClient != null) {
                clusterClient.close();
            }
        }
    }

    @Override
    public Boolean cancel(String applicationId) {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(getFlinkConfDir());
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        YarnClient yarnClient = YarnUtil.getYarnClient();
        YarnConfiguration yarnConf = (YarnConfiguration) yarnClient.getConfig();
        try (YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                flinkConfig,
                yarnConf,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);) {
            clusterDescriptor.killCluster(ApplicationId.fromString(applicationId));
            log.info("cancel flink job [{}] success!", applicationId);
            return true;
        } catch (FlinkException e) {
            log.error("cancel flink job error!", e);
        }
        return false;
    }

    @Override
    public List<FlinkJobStatus> list() {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(getFlinkConfDir());
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        YarnClient yarnClient = YarnUtil.getYarnClient();
        try {
            List<FlinkJobStatus> result = new ArrayList<>();
            List<ApplicationReport> applications = yarnClient.getApplications();
            for (ApplicationReport appReport : applications) {
                FlinkJobStatus status = FlinkJobStatus.builder()
                        .id(appReport.getApplicationId().toString())
                        .name(appReport.getName())
                        .status(appReport.getYarnApplicationState().toString())
                        .startTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(appReport.getStartTime()), ZoneId.systemDefault()))
                        .build();
                result.add(status);
            }
            log.info("flink job list : [{}]", result);
            return result;
        } catch (YarnException | IOException e) {
            throw new RuntimeException(e);
        }
    }


    private String getFlinkConfDir() {
        try {
            String path = EnvConfig.ENV + "/";
            URL url = YarnApplicationClient.class.getClassLoader().getResource(path);
            return url.toURI().getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ApplicationId> getJobApplicationId(YarnClient yarnClient, String applicationName) throws IOException, YarnException {
        List<ApplicationId> applicationIds = new ArrayList<>();
        // 定义过滤器，只获取 RUNNING 和 ACCEPTED 状态的应用程序
        EnumSet<YarnApplicationState> appStates = EnumSet.of(YarnApplicationState.RUNNING, YarnApplicationState.ACCEPTED);
        List<ApplicationReport> applications = yarnClient.getApplications(appStates);
        for (ApplicationReport appReport : applications) {
            if (appReport.getName().equals(applicationName)) {
                applicationIds.add(appReport.getApplicationId());
            }
        }
        return applicationIds;
    }
}
