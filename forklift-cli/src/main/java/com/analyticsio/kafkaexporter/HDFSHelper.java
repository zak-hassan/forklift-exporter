package com.analyticsio.kafkaexporter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

/**
 * Created by zhassan on 26/03/17.
 */
public class HDFSHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSHelper.class);

    String serverUrl;

    public HDFSHelper(String serverUrl){
        this.serverUrl=serverUrl;
    }

    public void mkdirHDFS(){
        String dir="/DEMO1";
        try {
            FileSystem filesystem= FileSystem.get( getHDFSConf(this.serverUrl));
            Path path = new Path(dir);
            if (filesystem.exists(path)) {
                LOGGER.info("Dir " + dir + " already exists!");
                return;
            }
            filesystem.mkdirs(path);
            filesystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void uploadFileHDFS(String source, String dest){
        try {
            FileSystem filesystem= FileSystem.get( getHDFSConf(serverUrl));
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
            if (dest.charAt(dest.length() - 1) != '/') {
                dest = dest + "/" + filename;
            } else {
                dest = dest + filename;
            }
            Path path = new Path(dest);
            if (filesystem.exists(path)) {
                LOGGER.info("File " + dest + " already exists");
                return;
            }
            FSDataOutputStream out = filesystem.create(path);
            InputStream in = new BufferedInputStream(new FileInputStream( new File(source)));
            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
            in.close();
            out.close();
            filesystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readHDFSFile(String file, String destFolder){
        try {
            FileSystem filesystem = FileSystem.get( getHDFSConf(serverUrl));
            Path path = new Path(file);
            if (!filesystem.exists(path)) {
                LOGGER.info("File " + file + " does not exists");
                return;
            }
            FSDataInputStream in = filesystem.open(path);
            String filename = file.substring(file.lastIndexOf('/') + 1, file.length());
            OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(destFolder+filename)));
            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
            in.close();
            out.close();
            filesystem.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Configuration getHDFSConf(String serverUrl) {
        //TODO: Make sure you export this HDFS uri as an environment variable or parameter to make this configurable.
        URI uri = URI.create("hdfs://"+serverUrl);
        Path path= new Path(uri);
        Configuration conf= new Configuration();

        System.setProperty("hadoop.home.dir", "/opt/hadoop");

        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/mapred-site.xml"));
        return conf;
    }
}
