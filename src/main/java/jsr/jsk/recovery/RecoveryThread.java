package jsr.jsk.recovery;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

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
import jsr.jsk.prpe.erasurecoding.SampleEncoder;
import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.GetRequest;
import jsr.jsk.prpe.thrift.GetResponse;
import jsr.jsk.prpe.thrift.PutRequest;
import jsr.jsk.prpe.thrift.PutResponse;

public class RecoveryThread {

	private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryThread.class);
	private HashMap<Integer, ArrayList<DataNodeLocation>> newBlockLocatoinsHashMap = null;
	
	public RecoveryThread() {
		newBlockLocatoinsHashMap = new HashMap<Integer, ArrayList<DataNodeLocation>>();
	}

	/**
	 * 
	 * @param blocksLostHashMap    number of nodes lost per block
	 * @param blockLocationHashMap location of each block
	 * @param blockTypeHashMap     type of each block
	 * @param availableEdges       remaining active edges
	 * @param deadEdges 		   Edges which have failed or inactive
	 */
	public HashMap<Integer, ArrayList<DataNodeLocation>> recover(
			HashMap<Integer, Integer> blocksLostHashMap,
			HashMap<Integer, ArrayList<DataNodeLocation>> blockLocationHashMap,
			HashMap<Integer, Integer> blockTypeHashMap, 
			ArrayList<DataNodeLocation> availableEdges,
			ArrayList<DataNodeLocation> deadEdges) {

		HashMap<Integer, ArrayList<DataNodeLocation>> newBlocksHashMap = new HashMap<Integer, ArrayList<DataNodeLocation>>();
		
		/** Lets start by iterating over blocks one by one **/
		for (Integer blockId : blocksLostHashMap.keySet()) {

			LOGGER.info("Recovering block : "+blockId);
			
			/** carefully update this structure **/
			newBlockLocatoinsHashMap.put(blockId, new ArrayList<DataNodeLocation>());
			
			Integer numBlocksLost = blocksLostHashMap.get(blockId);
			int blockType = blockTypeHashMap.get(blockId);
			ArrayList<DataNodeLocation> alreadyAllotedEdges = blockLocationHashMap.get(blockId);
			
			ArrayList<DataNodeLocation> newAllotedEdges = allotDataNodeLocations(numBlocksLost, availableEdges, alreadyAllotedEdges);
			
			/** Another important part **/
			newBlockLocatoinsHashMap.get(blockId).addAll(newAllotedEdges);
			
			for(DataNodeLocation myDataLoc : alreadyAllotedEdges) {
				if(deadEdges.contains(myDataLoc)==false) {
					newBlockLocatoinsHashMap.get(blockId).add(myDataLoc);
				}
			}
			
			if(blockType == Constants.REPLICATION) {
				recoverReplicatedBlock(blockId, alreadyAllotedEdges, newAllotedEdges, deadEdges);
			} else { /** Case of replication **/
				recoverErasureCodedBlock(blockId, alreadyAllotedEdges, newAllotedEdges, deadEdges);
			}
			
			LOGGER.info("Block : "+blockId+" recovered succesfully!");
		}/** End of for loop **/
		
		LOGGER.info("After recovery, new set of block and edge mapping are shown below ");
		for(Integer blockId : newBlockLocatoinsHashMap.keySet()) {
			LOGGER.info("The block id is "+blockId+" The locations are "+newBlockLocatoinsHashMap.get(blockId));
		}

		return newBlocksHashMap;
		
	}
	
	/**
	 * 
	 * @param numRequested
	 * @param availableEdges
	 * @param alreadyAllotedEdges
	 * @return set of newly alloted edges for writing data
	 */
	public ArrayList<DataNodeLocation> allotDataNodeLocations(
			int numRequested,
			ArrayList<DataNodeLocation> availableEdges, 
			ArrayList<DataNodeLocation> alreadyAllotedEdges) {

		LOGGER.info("The number of edges requested are "+numRequested);
		
		ArrayList<DataNodeLocation> newAllotedEdges = new ArrayList<DataNodeLocation>();
		ArrayList<DataNodeLocation> remEdgesList = new ArrayList<DataNodeLocation>();

		for (DataNodeLocation myDataLoc : availableEdges) {

			if (alreadyAllotedEdges.contains(myDataLoc) == false) /** This is an important condition **/
			{
				remEdgesList.add(myDataLoc);				
				LOGGER.info("The edgeID " + myDataLoc.getNodeid() + " is added for recovery of blocks");
			}

		}
		
		/**Now allot new Edges **/
		HashSet<Integer> myHashSet = new HashSet<Integer>();
		int numRand = remEdgesList.size(), count = 0;		

		while (count != numRequested) { /** Important field **/

			Random rand = new Random();
			int index = rand.nextInt(numRand);

			if (myHashSet.contains(index) == false) {
				count++; /** Incremented count **/
				myHashSet.add(index);

				DataNodeLocation myLoc = remEdgesList.get(index); /** Sheshadri has made a change here **/
				LOGGER.info("Case Replication : Added a location " + myLoc.getIp() + " : " + myLoc.getPort());
				newAllotedEdges.add(myLoc);
			}

		} /** End of while loop **/

		return newAllotedEdges;

	}

	/**
	 * 
	 * @param blockId
	 * @param alreadyAllotedEdges
	 * @param newAllotedEdges
	 * @param deadEdges
	 */
	public void recoverReplicatedBlock(Integer blockId, 
			ArrayList<DataNodeLocation> alreadyAllotedEdges,
			ArrayList<DataNodeLocation> newAllotedEdges, 
			ArrayList<DataNodeLocation> deadEdges) {

		/** First perform a get **/		
		byte[] data = null;
		boolean blockFetched = false;
		
		for(DataNodeLocation myDataLoc : alreadyAllotedEdges) {
			
			/** If the current edge is not a dead edge then retrieve the block **/
			if(deadEdges.contains(myDataLoc)==false) {
				
				String IP = myDataLoc.getIp();
				int port = myDataLoc.getPort();
				
				LOGGER.info("The node from which block is being recovered is "+myDataLoc.getNodeid()+" IP : "+IP+"  Port : "+port);
				
				/************************** Get Request **************************/ 
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
	    		
	    		GetRequest myGetReq = new GetRequest(blockId);
	    		GetResponse myGetResponse = null;
				try {
					myGetResponse = myClient.get(myGetReq);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		
	    		if(myGetResponse.getStatus()==Constants.SUCCESS) {
	    			
	    			data = myGetResponse.getData();
	    			blockFetched = true;
	    			String blockNumString = myGetResponse.getBlockNumber(); /** This is sent as a response **/
	    			
	    			try {
	    				
	    				FileOutputStream myFileOutput = new FileOutputStream(new File(Constants.OUTPUT_DIR+blockNumString));
		    			myFileOutput.write(data);
		    			myFileOutput.close();
		    			
	    			}catch(IOException e) {
	    				e.printStackTrace();
	    			}
	    				    			
	    			LOGGER.info("Block "+ blockNumString +" read by replication num bytes : "+data.length);
	    			break;
	    		}
	    		
	    		transport_edge.close();
	    		/************************** Get Request **************************/
				
			} /** End of If loop **/
			
			if(blockFetched==true) {
				LOGGER.info("Block is succesfully fetched breaking from the loop");
				break;
			}				
			
		}/** End of for loop **/
		
		/** Put the block in the newly alloted locations **/
		
		for(DataNodeLocation myDataLoc : newAllotedEdges) {
			
			String IP = myDataLoc.getIp();
			int port = myDataLoc.getPort();
			
			LOGGER.info("The new Edge where the data is being written to is "+myDataLoc.getNodeid()+" IP : "+IP+"  Port : "+port);
			
			/************************** Put Request **************************/ 
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
    		
    		
    		PutRequest myPutReq =  new PutRequest(blockId+"",ByteBuffer.wrap(data));
    		try {
    			
				PutResponse myRes = myClient.put(myPutReq);
				if(myRes.getResponse()==Constants.SUCCESS) {
					LOGGER.info("Recovery block : "+blockId+ " replication successfully written ");
				}
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		transport_edge.close();
    		/************************** Put Request **************************/
			
		}/** End of for loop **/
	}

	/**
	 * 
	 * @param blockId
	 * @param alreadyAllotedEdges
	 * @param newAllotedEdges
	 * @param deadEdges
	 */
	public void recoverErasureCodedBlock(Integer blockNumber,
			ArrayList<DataNodeLocation> alreadyAllotedEdges, 
			ArrayList<DataNodeLocation> newAllotedEdges, 
			ArrayList<DataNodeLocation> deadEdges) {

		/** First perform a get **/
		byte[] data = null;
		boolean dataFetched = false;
		LOGGER.info("Block number trying to be recovered is "+blockNumber);
		SampleDecoder myDecoder = new SampleDecoder();
		
		try {
			data = myDecoder.decodeRecovery(blockNumber, alreadyAllotedEdges, deadEdges, newAllotedEdges);
			if(data!=null) {
				dataFetched = true;
				LOGGER.info("Recovery Erasure coded, get success "+blockNumber);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ArrayList<String> lostErasureCodedBlocks = myDecoder.getLostBlocksList();
		
		/** Put Request of the erasure coded block **/
		SampleEncoder myEncoder = new SampleEncoder();
		try {
			myEncoder.encodeRecovery(blockNumber+"", lostErasureCodedBlocks, newAllotedEdges, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
