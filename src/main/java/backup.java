import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

@SuppressWarnings("unused")
public class backup {
  public static void main(String[] args) throws InterruptedException, UnknownHostException {
	String oldSnapshot = "";
	String newSnapshot = "";
	
	InetAddress IP=InetAddress.getLocalHost();
	String localIP = IP.getHostAddress();
	
	File workingdirectory = new File(System.getProperty("user.home")
	  + "/cassandra/");
	
	String nodeList = args[1];
	String[] nodeArray = new String[5];
	
	for (int i = 1; i < args.length; i++) {
	  nodeList = nodeList + " " + args[i];
	  nodeArray[i - 1] = args[i];
	}
	
	try {
	  System.out.println("Deleting old backup folders...");
	  Process deleteFolder = Runtime.getRuntime().exec("rm -rf "
	    + workingdirectory + "/backup");
	  deleteFolder.waitFor();
	  System.out.println("Deleted");
	  
	  System.out.println("Making new folders...");
	  Process makefolder = Runtime.getRuntime().exec("mkdir "
	    + workingdirectory +  "/backup");
	  makefolder.waitFor();
		
	  for (int i = 1; i < args.length; i++) {
	    Process makefolder2 = Runtime.getRuntime().exec("mkdir "
	      + workingdirectory +  "/backup/" + args[i]);
	    makefolder2.waitFor();
	  }
	  System.out.println("New folders made");
	}
	
	catch (IOException e) {
	  e.printStackTrace();
	}
	
	/*String directory = "/var/lib/cassandra/data/" + args[0]
	  + "/data/snapshots/";
	
	File folder = new File("/var/lib/cassandra/data/" + args[0]
	  + "/data/snapshots/");
    File[] flist = folder.listFiles();
    oldSnapshot = flist[0].getName();*/
    
    try {
      System.out.println("Removing all snapshots...");
	  Process clearsnapshot = Runtime.getRuntime().exec("pssh -h hostlist.txt "
	    + "nodetool clearsnapshot " + args[0]);
	  clearsnapshot.waitFor();
	  System.out.println("Snapshots removed");
	  
	  System.out.println("Taking new snapshot...");
	  Process snapshot = Runtime.getRuntime().exec("pssh -h hostlist.txt "
	    + "nodetool snapshot " + args[0]);
	  snapshot.waitFor();
	  System.out.println("Snapshot taken");
	  
    }
    catch (IOException e) {
	  e.printStackTrace();
	}
    
    try {      
      Process process = Runtime.getRuntime().exec("hadoop fs -mkdir "
        + "/user/hdfs/cassandrabackup/");
      process.waitFor();
      
  	  if (true == false) {
  	    //Check here if new and old backup folders are equal. If so, delete the
  		  //old folder and do nothing else.
  	  }
  	  
  	  else {
  		System.out.println("Getting new backup using rsync");
  		for (int i = 0; i < nodeArray.length; i++) {
  		  System.out.println("Getting backup from " + nodeArray[i] + "...");
  		  Process getBackup = Runtime.getRuntime().exec("ssh root@"
  		    + nodeArray[i] + " rsync -r /var/lib/cassandra/data/" + args[0]
  		      + "/data/snapshots/* " + "root@" + localIP + ":"
  		        + workingdirectory + "/backup/" + nodeArray[i]);
  		  getBackup.waitFor();
  		}
  		
  		try {
  		  System.out.println("Deleting old backup from Hadoop...");
  		  Process deleteHadoopFolders = Runtime.getRuntime().exec("hadoop fs "
  		    + "-rm -r /user/hdfs/cassandrabackup/*");
  		  deleteHadoopFolders.waitFor();
  		  
  		  System.out.println("Old backup deleted, writing new backup "
  		  	+ "to Hadoop...");
  		  
  		try {
            FileSystem hdfs=FileSystem.get(new Configuration());

            //Get home directory
            Path homeDir=hdfs.getHomeDirectory();
            //Set HDFS path
            Path newFolderPath= new Path(homeDir + "");
            //Set local path
            if(!hdfs.exists(newFolderPath)) {
              hdfs.mkdirs(newFolderPath);
            }
            System.out.println(workingdirectory.getAbsolutePath() + "/backup/*");
            Path localFilePath = new Path(workingdirectory.getAbsolutePath() +
              "/backup/*");
            //Set HDFS file location
            Path hdfsFilePath=new Path(newFolderPath + "/cassandrabackup");

            hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
          }

          catch (Exception ex) {
            Thread t = Thread.currentThread();
            t.getUncaughtExceptionHandler().uncaughtException(t, ex);
          }
  		  
  		  System.out.println("Backup written to Hadoop");
  		}
  	    catch (IOException e) {
  		  e.printStackTrace();
  		}
  	  }
  	}
    catch (IOException e) {
  	  e.printStackTrace();
  	}
  }
}