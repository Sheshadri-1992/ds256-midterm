package jsr.jsk.prpe.edge;

import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.servicehandler.EdgeServiceHandler;

import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.MasterService;
import jsr.jsk.prpe.thrift.RegisterEdgeRequest;
import jsr.jsk.prpe.thrift.RegisterEdgeResponse;

public class EdgeServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(EdgeServer.class);
	public static EdgeServiceHandler masterServiceHandler = null;
	public static EdgeService.Processor eventProcessor = null;
	public static Edge myEdge = null;
	public static int nodeId =0;
	public static String IP = "";
	public static int port = 0; 
	
	
	public static void main(String[] args) {	
		
		if(args.length!=3) {
			LOGGER.info("Usage 3 args needed , id , ip and port");
			System.exit(0);
		}		
		nodeId = (int)Integer.valueOf(args[0]);
		IP = args[1];
		port = (int)Integer.valueOf(args[2]);
		
		masterServiceHandler = new EdgeServiceHandler(nodeId);
		eventProcessor = new EdgeService.Processor(masterServiceHandler);
		
		myEdge = new Edge(nodeId, Constants.RELIABILITY);
		
		/** First register the device with master **/
		registerEdgeServer();
		
		Runnable heartBeatRunnable = new Runnable() {			
			@Override
			public void run() {
			
				while(true) {
					myEdge.sendHeartBeat();
					try {
						Thread.sleep(Constants.HEARTBEAT_INTERVAL);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				
			}
		};
				
		Runnable masterRunnable = new Runnable() {
			
			@Override
			public void run() {

				startEdgeServer(eventProcessor, port );
			}
		};
			
		/** start listening and expose services **/
		Thread masterThread = new Thread(masterRunnable);
		masterThread.start();
		
		/**Thread to keep sending heart beats **/
		Thread heartBeatThread = new Thread(heartBeatRunnable);
		heartBeatThread.start();

	}
	
	/**
	 * Add yourself to master's list
	 */
	public static void registerEdgeServer() {
		TTransport transport = new TFramedTransport(new TSocket(Constants.MASTER_IP, Constants.MASTER_PORT));
		try {
			transport.open();
		} catch (TTransportException e) {
			transport.close();
			LOGGER.error("Error opening connection to Master IP : {} and port : {}", Constants.MASTER_IP, Constants.MASTER_PORT);
			e.printStackTrace();
			return;
		}
		
		TProtocol protocol = new TBinaryProtocol(transport);
		MasterService.Client masterClient = new MasterService.Client(protocol);
		LOGGER.info("Registering device with master ");
		
		RegisterEdgeRequest registerReq = new RegisterEdgeRequest();
		
		DataNodeLocation dnloc = new DataNodeLocation(nodeId,IP, port);
		registerReq.setNodeId(nodeId);
		registerReq.setEdgeLocation(dnloc);
		
		RegisterEdgeResponse response = null;
		
		try {
			response = masterClient.registerEdge(registerReq);
			LOGGER.info("The Register Edge Response is "+response.getStatus());
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Start listening to clients
	 */
	public static void startEdgeServer(EdgeService.Processor eventProcessor, int serverPort) {
		try {
			TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(serverPort);
			/*TServer server = new TNonblockingServer(
					new TNonblockingServer.Args(serverTransport).processor(eventProcessor));*/
			TThreadedSelectorServer.Args serverArgs = new TThreadedSelectorServer.Args(serverTransport);
			//20 threads for processing requests
			serverArgs.executorService(Executors.newFixedThreadPool(20));
			serverArgs.processor(eventProcessor);
			TThreadedSelectorServer server = new TThreadedSelectorServer(serverArgs);
			LOGGER.info("Starting the Edge Server.. ");
			server.serve();
			LOGGER.info("Closed the connection Thrift Server");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
