package jsr.jsk.prpe.master;

import java.util.HashMap;
import java.util.concurrent.Executors;

import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.miscl.MyParser;
import jsr.jsk.prpe.servicehandler.MasterServiceHandler;
import jsr.jsk.prpe.thrift.MasterService;

public class MasterServer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MasterServer.class);
	public static MasterServiceHandler masterServiceHandler = null;
	public static MasterService.Processor eventProcessor = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello World!");
		
		MyParser parser = new MyParser();
		HashMap<String,String> masterLoc = parser.returnMasterLocation();
		
		String masterIp = "127.0.0.1";
		Integer masterPort = 8080;
		
		if(masterLoc!=null) {
			masterIp = masterLoc.get("ip");
			masterPort = Integer.parseInt(masterLoc.get("port"));
		}
				
		masterServiceHandler = new MasterServiceHandler();
		eventProcessor = new MasterService.Processor(masterServiceHandler);
		
		Runnable masterRunnable = new Runnable() {
			
			@Override
			public void run() {

				startMasterServer(eventProcessor, Constants.MASTER_PORT );
			}
		};
		
		/** start listening and expose services **/
		Thread masterThread = new Thread(masterRunnable);
		masterThread.start();
		
	}
	
	/**
	 * Start listening to clients
	 */
	public static void startMasterServer(MasterService.Processor eventProcessor, int serverPort) {
		try {
			TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(serverPort);
			/*TServer server = new TNonblockingServer(
					new TNonblockingServer.Args(serverTransport).processor(eventProcessor));*/
			TThreadedSelectorServer.Args serverArgs = new TThreadedSelectorServer.Args(serverTransport);
			//20 threads for processing requests
			serverArgs.executorService(Executors.newFixedThreadPool(20));
			serverArgs.processor(eventProcessor);
			TThreadedSelectorServer server = new TThreadedSelectorServer(serverArgs);
			LOGGER.info("Starting the Master Server.. ");
			server.serve();
			LOGGER.info("Closed the connection Thrift Server");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
