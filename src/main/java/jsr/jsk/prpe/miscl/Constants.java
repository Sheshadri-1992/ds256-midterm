package jsr.jsk.prpe.miscl;

public class Constants {
	
	public static final int MASTER_PORT = 8080;
	public static final String MASTER_IP = "127.0.0.1";
	public static final int HEALTHY = 1;
	public static final int UNHEALTHY = 0;
	public static final double RELIABILITY = 0.95;
	public static final int SUCCESS = 1;
	public static final int FAILURE = 0;
	public static final int BLOCKSIZE = 1*1024*1024;
	public static final int REPLICATION = 1;
	public static final int ERASURE_CODING = 2;
	public static final int NUM_REPLICATION = 3;
	public static final int NUM_ERASURE_CODING = 6;
	public static final String EDGE_DIR = "./data";
	public static final int HEARTBEAT_INTERVAL = 15000;
	public static final int HEARTBEAT_CHECK_INTERVAL = 15000;
	public static final int RECOVERY_INTERVAL = 60000;
	public static final String DIRECTORY_PATH = "./input/";
	public static final String OUTPUT_DIR = "./output/";
	public static final String TEMP_DIR = "./temp/";
	public static final String RECOVERY_DIR = "./recovery/";
	public static final String LOGS_DIR = "./Logs/";
	public static final int WRITE_REQUEST = 1;
	public static final int ALLOWED_HB_MISS = 3;
}
