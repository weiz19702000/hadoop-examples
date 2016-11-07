package com.weizhang.hadoop.samples.putmerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMergeMain {

	public static void main(String[] args) {
		test1(args);
	}
		
	private static void test1(String[] args) {
		try {
			String inputDirPath = args[1];
			String outputFilePath = args[0];
			
			Configuration config = new Configuration();
			/*
			 * It is important to specify fs.default.name to the namenode. Otherwise, later it will create the output
			 * hdfs FileSystem as local file system.
			 */
			config.set("fs.default.name", "hdfs://localhost:9000");
			
			FileSystem hdfs = FileSystem.get(config);
			FileSystem local = FileSystem.getLocal(config);
			
			Path inputDir = new Path(inputDirPath);
			Path hdfsFile = new Path(outputFilePath);
			
			FileStatus[] inputFiles = local.listStatus(inputDir);
			FSDataOutputStream out = hdfs.create(hdfsFile, true);
			
			for (FileStatus inputFile : inputFiles) {
				System.out.println(inputFile.getPath().getName());
				FSDataInputStream in = local.open(inputFile.getPath());
				
				int size = 1024;
				byte buffer[] = new byte[size];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
