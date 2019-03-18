package jsr.jsk.prpe.client;

import java.io.File;
import java.io.FileInputStream;
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

import jsr.jsk.prpe.erasurecoding.SampleEncoder;
import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.MasterService;
import jsr.jsk.prpe.thrift.OpenFileRequest;
import jsr.jsk.prpe.thrift.OpenFileResponse;
import jsr.jsk.prpe.thrift.PutRequest;
import jsr.jsk.prpe.thrift.PutResponse;
import jsr.jsk.prpe.thrift.WriteBlockRequest;
import jsr.jsk.prpe.thrift.WriteBlockResponse;

public class PutRequestClass {

	private String inputFileName= "";
	private String inputDirPath = "";
	private double storageBudget = 1.5;
	private int sessionHandle = 0;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PutRequestClass.class);
	
	public PutRequestClass(String argFileName, double argStorageBudget) {
		inputFileName = argFileName;
		inputDirPath = Constants.DIRECTORY_PATH;
		storageBudget = argStorageBudget;
	}
	
	public void putFile() {
		
		LOGGER.info("A request for put has come filename "+inputFileName);
		sessionHandle = openFileRequest(); 	/** Session handle is set here **/
		writeFile();
	}
	
	public int getSessionHandle() {
		return sessionHandle;
	}
	
	/**
	 * 
	 * @return The session handle returned by the master
	 */
	private int openFileRequest() {
		int filesize = 0;
		File myFile = new File(inputDirPath+inputFileName);
		
		LOGGER.info("The input file path given is "+inputDirPath+inputFileName+ " : The storage budget is "+storageBudget);
		if(myFile.exists()) {
			filesize = (int)myFile.length();
		}
				
		OpenFileRequest myRequest = new OpenFileRequest(); /** Openrequest has 4 required fields so I set them **/
		myRequest.setFilename(inputFileName);
		myRequest.setFilesize(filesize);
		myRequest.setStoragebudget(storageBudget);
		myRequest.setRequesttype(Constants.WRITE_REQUEST);
		
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
		LOGGER.info("OpenFile Request with master ");		
		
		OpenFileResponse myResponse = null;
		
		try {
			
			myResponse = masterClient.openFile(myRequest);
			int returnedHandle = myResponse.getHandle();
			LOGGER.info("The returned handle is "+returnedHandle);
			return returnedHandle;
			
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		
	}
	
	/**
	 * Takes file name and puts it to the data store
	 */
	private void writeFile() {
		
		int filesize = 0;
		File myFile = new File(inputDirPath+inputFileName);		
		filesize = (int)myFile.length(); /** This is the error which was giving problem **/
		
		
		double numBlocks = filesize / (1.0 *Constants.BLOCKSIZE);		
		double integerPart = (int) Math.floor(numBlocks);		
		double fractionPart = numBlocks - integerPart;
		
		int fullBlocks =0, partialBlocks = 0;
		
		if(fractionPart==0.0) {
			fullBlocks = (int) integerPart;
		}else {
			fullBlocks = (int) integerPart;
			partialBlocks = 1;
		}
		
		try {
			FileInputStream myFileInput = new FileInputStream(myFile);
			int index = 0;
			
			while(index<fullBlocks) {
				
				byte[] buffer = new byte[Constants.BLOCKSIZE];				
				myFileInput.read(buffer);
				
				
				writeBlocks(buffer);
				LOGGER.info("The bytes about to be written are "+buffer.length);
				index++; /** very important **/
			}/** End of while loop **/
			
			int remainingBytes = filesize - fullBlocks*Constants.BLOCKSIZE;			
			byte[] buffer = new byte[remainingBytes];			
			myFileInput.read(buffer);
			
			
			writeBlocks(buffer);
			myFileInput.close();			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private void writeBlocks(byte[] data) {
		
		WriteBlockRequest myWriteBlockReq = new WriteBlockRequest(sessionHandle); /** Session handle is passed here **/
		

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
		LOGGER.info("WriteBlock Request with master ");
		
		WriteBlockResponse response = null;
		
		try {
			response = masterClient.requestBlocksToWrite(myWriteBlockReq);
			
			int type = response.getType();
			int blockNum = response.getBlocknum();
			ArrayList<DataNodeLocation> myDataLocs = (ArrayList<DataNodeLocation>) response.getLocations(); 
			
			LOGGER.info("The type is "+type+" blockNum is "+blockNum);
			for(DataNodeLocation myDataloc : myDataLocs) {
				LOGGER.info("The location is "+myDataloc.getNodeid()+" : "+myDataloc.getIp()+ " : "+myDataloc.getPort());
			}
			
			if(type==Constants.REPLICATION) {
				
				writeConcurrently(Constants.NUM_REPLICATION, Constants.REPLICATION,blockNum, data, myDataLocs);
				
			}else {
				
				writeConcurrently(Constants.NUM_ERASURE_CODING, Constants.ERASURE_CODING, blockNum, data, myDataLocs);
			}
			
			
		} catch (TException e) {
		
			e.printStackTrace();
		}
		
	}
	
	
	//TODO : multi-threading has to be done still
	public void writeConcurrently(int numThreads, int type,int blockNum ,byte[] data,ArrayList<DataNodeLocation> myDataLocs) {
		
		try {
			FileOutputStream myOutStream = new FileOutputStream(new File(blockNum+""));
			myOutStream.write(data);
			myOutStream.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(type==Constants.ERASURE_CODING) { /**ERASURE CODING **/
			SampleEncoder myEncoder = new SampleEncoder();
			try {
				LOGGER.info("Case of Erasure coding");
				myEncoder.encode(blockNum+"", myDataLocs, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else { /** REPLICATION **/
			LOGGER.info("Case of replication");
			
			for(DataNodeLocation myDataLoc : myDataLocs) {
				
				String IP = myDataLoc.getIp();
				int port = myDataLoc.getPort();
				
				TTransport transport = new TFramedTransport(new TSocket(IP, port));
	    		try {
	    			transport.open();
	    		} catch (TTransportException e) {
	    			transport.close();
	    			LOGGER.error("Error opening connection to Master IP : {} and port : {}", IP, port);
	    			e.printStackTrace();
	    			return;
	    		}
	    		
	    		TProtocol protocol = new TBinaryProtocol(transport);
	    		EdgeService.Client myClient = new EdgeService.Client(protocol);

	    		PutRequest myPutReq = new PutRequest(blockNum+"", ByteBuffer.wrap(data));
	    		try {
					PutResponse myRes = myClient.put(myPutReq);
					if(myRes.getResponse()==Constants.SUCCESS) {
						LOGGER.info("Replicated block successfully written ");;
					}
					
				} catch (TException e) {
				
					e.printStackTrace();
				}
			}
		}
		
	}
	
}
