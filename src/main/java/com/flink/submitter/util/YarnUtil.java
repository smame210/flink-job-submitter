package com.flink.submitter.util;

import com.flink.submitter.config.EnvConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * @author daitf
 * @date 2025/1/13
 */
public class YarnUtil {
    public static final String CORE_SITE = "/conf/core-site.xml";
    public static final String YARN_SITE = "/conf/yarn-site.xml";
    public static final String HDFS_SITE = "/conf/hdfs-site.xml";

    private static YarnClient yarnClient = null;

    public static synchronized YarnClient getYarnClient() {
        if (yarnClient == null) {
            yarnClient = YarnClient.createYarnClient();

            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            String profile = EnvConfig.ENV;
            conf.addResource(profile + CORE_SITE);
            conf.addResource(profile + YARN_SITE);
            conf.addResource(profile + HDFS_SITE);


            YarnConfiguration yarnConfiguration = new YarnConfiguration(conf);
            yarnClient.init(yarnConfiguration);
            yarnClient.start();
        }

        return yarnClient;
    }
}
