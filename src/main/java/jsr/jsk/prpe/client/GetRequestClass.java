package jsr.jsk.prpe.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.erasurecoding.SampleDecoder;
import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.thrift.BlockLocation;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.GetRequest;
import jsr.jsk.prpe.thrift.GetResponse;
import jsr.jsk.prpe.thrift.MasterService;
import jsr.jsk.prpe.thrift.ReadBlockRequest;
import jsr.jsk.prpe.thrift.ReadBlockResponse;

public class GetRequestClass {

	private String filename = ""; 
	private static final Logger LOGGER = LoggerFactory.getLogger(GetRequestClass.class);
	
	public GetRequestClass(String argFilename) {
		
		filename = argFilename;
		
		File myDir = new File(Constants.OUTPUT_DIR);
		if(myDir.exists()==false) {
			boolean result = myDir.mkdir();
			LOGGER.info("Output Directory creation "+result);
		}
				
	}
	
	public void getFileReq() {
		
		LOGGER.info("Get request for file "+filename);
	
		TTransport transport = new TFramedTransport(new TSocket(Constants.MASTER_IP, Constants.MASTER_PORT));
		try {
			transport.open();
		} catch (TTransportException e) {
			transport.close();
			LOGGER.error("Error opening connection to Master IP : {} and port : {}", Constants.MASTER_IP, Constants.MASTER_PORT);
			e.printStackTrace();
		}
		
		TProtocol protocol = new TBinaryProtocol(transport);
		MasterService.Client masterClient = new MasterService.Client(protocol);
		LOGGER.info("Read File Request with master ");
		
		ReadBlockRequest myReq = new ReadBlockRequest(filename);
		ReadBlockResponse response = null;
		try {
			response = masterClient.requestBlocksToRead(myReq);
			transport.close(); /**close connection**/
			
			LOGGER.info("The Response is "+response.getStatus());
			
			ArrayList<BlockLocation> myBlockLocs = (ArrayList<BlockLocation>) response.getBlocklocations();
			
			for(BlockLocation blockloc : myBlockLocs) {/** READ MANY BLOCKS **/
				
				int type = blockloc.getType();
				int blocknum = blockloc.getBlocknumber();
				ArrayList<DataNodeLocation> myDataLocs  = (ArrayList<DataNodeLocation>) blockloc.getLocations();
				
				if(type==Constants.ERASURE_CODING) {
					
					SampleDecoder myDecoder = new SampleDecoder();
					LOGGER.info("Get block from erasure coding ");
					myDecoder.decode(blocknum+"", blocknum, myDataLocs);
					
				}else { /**REPLICATION **/ /**WATC OUT HERE **/
					
					LOGGER.info("Get block from replication");
				
					for(DataNodeLocation myDataLoc : myDataLocs) {
						
						String IP = myDataLoc.getIp();
						int port = myDataLoc.getPort();
						
						TTransport transport_edge = new TFramedTransport(new TSocket(IP, port));
			    		try {
			    			transport_edge.open();
			    		} catch (TTransportException e) {
			    			transport_edge.close();
			    			LOGGER.error("Error opening connection to Master IP : {} and port : {}", IP, port);
			    			e.printStackTrace();
			    			return;
			    		}
			    		
			    		TProtocol protocol_edge = new TBinaryProtocol(transport_edge);/** THIS was causing a problem **/
			    		EdgeService.Client myClient = new EdgeService.Client(protocol_edge);/** THIS was causing a problem **/
			    		
			    		GetRequest myGetReq = new GetRequest(blocknum);
			    		GetResponse myGetResponse = myClient.get(myGetReq);
			    		
			    		if(myGetResponse.getStatus()==Constants.SUCCESS) {
			    			
			    			byte[] data = myGetResponse.getData();
			    			String blockNumString = myGetResponse.getBlockNumber(); /** This is sent as a response **/
			    			
			    			FileOutputStream myFileOutput = new FileOutputStream(new File(Constants.OUTPUT_DIR+blockNumString));
			    			myFileOutput.write(data);
			    			myFileOutput.close();
			    			
			    			LOGGER.info("Block "+ blockNumString +" read by replication num bytes : "+data.length);
			    			break;
			    		}
			    		
			    		transport_edge.close();
					}
					
				}
				
			}
			
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
