package org.apache.hadoop.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HDFSFileStatusExample {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSFileStatusExample.class.getName());

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 0) {
            System.err.println("Usage: HDFSFileStatus <hdfs-file-path> [<hdfs-file-path> ...]");
            System.exit(1);
        }

        Configuration configuration = new Configuration();

        // Set up Kerberos authentication
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("hadoop.security.authorization", "true");

        configuration.set("hadoop.http.authentication.type", "kerberos");
        System.out.println("hadoop.security.authentication:  " + configuration.get("hadoop.security.authentication"));

        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab(args[1], args[2]);
            System.out.println("LOGGED IN");
        } catch (IOException e) {
            LOG.info(e.getMessage());
        }


        configuration.iterator().forEachRemaining(e -> LOG.info("Configuration: {}={}", e.getKey(), e.getValue()));


        int threadCount = Integer.parseInt(args[3]);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(new SomeTask(args[0], configuration, Boolean.parseBoolean(args[4])));
        }

        boolean terminated = executorService.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("Terminated=" + terminated);


    }



    static class SomeTask implements Runnable {
        private final String uri;
        Configuration conf;
        boolean isProxyUser;

        SomeTask(String hdfs_uri, Configuration conf, boolean isProxyUser) {
            this.uri = hdfs_uri;
            this.conf = conf;
            this.isProxyUser = isProxyUser;
        }

        @Override
        public void run() {

            try {
                while (true) {
                    URI uris = new URI(uri);
                    UserGroupInformation currentUserUgi = UserGroupInformation.getCurrentUser();

                    UserGroupInformation proxyUserUGI = UserGroupInformation.createProxyUser(
                            "hive", UserGroupInformation.getLoginUser());

                    LOG.info("Current user: " + currentUserUgi.getUserName());
                    LOG.info("Authentication method: " + currentUserUgi.getAuthenticationMethod());

                    // Check if the authentication method is KERBEROS
                    if (currentUserUgi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS) {
                        LOG.info("UGI Successfully authenticated using Kerberos.");
                    } else {
                        LOG.info("UGI Failed to authenticate using Kerberos.");
                    }

                    // Check if the authentication method is KERBEROS
                    if (proxyUserUGI.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY) {
                        LOG.info("SESSIONUGI Proxy");
                    } else {
                        LOG.info("SESSIONUGI no Proxy");
                    }


                    LOG.info("ugi is " + currentUserUgi.toString());

                    LOG.info("ProxyUgi is " + proxyUserUGI.toString());


                    FileSystem hdfs1 = FileSystem.get(uris, conf);
                    LOG.info("hdfs1 is : {} ", hdfs1.toString());

                    UserGroupInformation finalUGI;
                    if (isProxyUser) {
                        finalUGI = proxyUserUGI;
                    } else {
                        finalUGI = currentUserUgi;
                    }


                    finalUGI.doAs((PrivilegedExceptionAction<Void>) () -> {
                        FileSystem hdfs = FileSystem.get(uris, conf);
                        LOG.info("hdfs is : {} ", hdfs.toString());

                        Path path0 = new Path("/warehouse/tablespace/external/hive/concurrency.db");
                        Path path1 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_1");
                        Path path2 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_2");
                        Path path3 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_3");
                        Path path4 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_4");
                        Path path5 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_5");
                        Path path6 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_6");
                        Path path7 = new Path("/warehouse/tablespace/external/hive/concurrency.db/table_7");
                        Path path8 = new Path("/data");

                        try {

                            FileStatus fileStatus0 = hdfs.getFileStatus(path0);
                            LOG.info("fileStatus0 : {} ", fileStatus0.toString());
                            FileStatus fileStatus1 = hdfs.getFileStatus(path1);
                            LOG.info("fileStatus1 : {} ", fileStatus1.toString());
                            FileStatus fileStatus2 = hdfs.getFileStatus(path2);
                            LOG.info("fileStatus2 : {} ", fileStatus2.toString());
                            FileStatus fileStatus3 = hdfs.getFileStatus(path3);
                            LOG.info("fileStatus3 : {} ", fileStatus3.toString());
                            FileStatus fileStatus4 = hdfs.getFileStatus(path4);
                            LOG.info("fileStatus4 : {} ", fileStatus4.toString());
                            FileStatus fileStatus5 = hdfs.getFileStatus(path5);
                            LOG.info("fileStatus5 : {} ", fileStatus5.toString());
                            FileStatus fileStatus6 = hdfs.getFileStatus(path6);
                            LOG.info("fileStatus6 : {} ", fileStatus6.toString());
                            FileStatus fileStatus7 = hdfs.getFileStatus(path7);
                            LOG.info("fileStatus7 : {} ", fileStatus7.toString());
                            FileStatus fileStatus8 = hdfs.getFileStatus(path8);
                            LOG.info("fileStatus8 : {} ", fileStatus8.toString());
                        } catch (Exception e) {
                            System.out.println("Exception is : " + e.getMessage());
                        }

                        return null;
                    });
                    System.out.println("-----------------------------------------------------");
                }

            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}