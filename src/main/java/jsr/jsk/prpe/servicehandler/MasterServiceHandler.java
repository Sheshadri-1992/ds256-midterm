package jsr.jsk.prpe.servicehandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.miscl.MyParser;
import jsr.jsk.prpe.thrift.BlockLocation;
import jsr.jsk.prpe.thrift.BlockReportRequest;
import jsr.jsk.prpe.thrift.BlockReportResponse;
import jsr.jsk.prpe.thrift.CloseFileRequest;
import jsr.jsk.prpe.thrift.CloseFileResponse;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.HeartBeatRequest;
import jsr.jsk.prpe.thrift.HeartBeatResponse;
import jsr.jsk.prpe.thrift.ListFileRequest;
import jsr.jsk.prpe.thrift.ListFileResponse;
import jsr.jsk.prpe.thrift.MasterService;
import jsr.jsk.prpe.thrift.OpenFileRequest;
import jsr.jsk.prpe.thrift.OpenFileResponse;
import jsr.jsk.prpe.thrift.ReadBlockRequest;
import jsr.jsk.prpe.thrift.ReadBlockResponse;
import jsr.jsk.prpe.thrift.RegisterEdgeRequest;
import jsr.jsk.prpe.thrift.RegisterEdgeResponse;
import jsr.jsk.prpe.thrift.WriteBlockRequest;
import jsr.jsk.prpe.thrift.WriteBlockResponse;
import jsr.jsk.recovery.HeartBeatCheckThread;
import jsr.jsk.recovery.RecoveryThread;

public class MasterServiceHandler implements MasterService.Iface {

	/** Filename is sessionHandle **/
	public static HashMap<Integer, String> fileSessionHashMap;
	
	/** File and set of blocks **/
	public static HashMap<String, ArrayList<Integer>> fileBlockHashMap;
	
	/** Block number and List of edge locations **/
	public static HashMap<Integer, ArrayList<DataNodeLocation>> blockLocationsHashMap;
	
	/** Holds a mapping between block number and type **/
	public static HashMap<Integer, Integer> blockTypeHashMap;
	
	/** tells per session how many replicated blocks are to be awarded **/
	public static HashMap<Integer, Integer> replicatedBlockHashMap;
	
	/** Edge and the blocks they hold **/
	public static HashMap<Integer, List<Integer>> edgeBlockListHashMap;
	
	/** Set of edge devices **/
	public static HashMap<Integer,DataNodeLocation> edgeDevicesList;
	
	/** Edge device to heart beat missing mapping **/
	public static ConcurrentHashMap<Integer, Long> edgeHeartbeatHashMap;
	
	/** A hashmap to keep track of number of heartbeats missed per edge **/
	public static ConcurrentHashMap<Integer, Integer> edgeHeartbeatsMissedHashMap;

	/** A class which checks hearbeats missed **/
	public static HeartBeatCheckThread myHbcheck;
	
	/** Recovery instance **/
	public static RecoveryThread myRecovery;
	
	public static List<Integer> edgesToRecover;
	
	public static Integer globalBlockNum = 0;
	private static final Logger LOGGER = LoggerFactory.getLogger(MasterServiceHandler.class);

	/** Initialize all the structures **/
	public MasterServiceHandler() {

		LOGGER.info("The in-memory structures are initialized ");

		fileSessionHashMap = new HashMap<Integer, String>();
		fileBlockHashMap = new HashMap<String, ArrayList<Integer>>();
		blockLocationsHashMap = new HashMap<Integer, ArrayList<DataNodeLocation>>();
		blockTypeHashMap = new HashMap<Integer, Integer>();
		replicatedBlockHashMap = new HashMap<Integer, Integer>();
		edgeBlockListHashMap = new HashMap<Integer, List<Integer>>();
		edgeDevicesList = new HashMap<Integer, DataNodeLocation>();
		edgeHeartbeatHashMap = new ConcurrentHashMap<Integer, Long>();
		edgeHeartbeatsMissedHashMap = new ConcurrentHashMap<Integer, Integer>();
		edgesToRecover = new ArrayList<Integer>();
		myHbcheck = new HeartBeatCheckThread();
		myRecovery = new RecoveryThread();

		Runnable hbRunnable = new Runnable() {
			
			@Override
			public void run() {
				
				while(true) {

					ArrayList<Integer> tempRecover = myHbcheck.updateHeartBeat(edgeHeartbeatHashMap);
					
					if(tempRecover.size()>0) {
						LOGGER.info("The dead edges are "+tempRecover);
						edgesToRecover.addAll(tempRecover);
					}
					LOGGER.info("The number of edges dead is " + edgesToRecover.size());
					if (edgesToRecover.size() > 0) {

						for (Integer edgeId : edgesToRecover) {

							LOGGER.info("Removing entries for hearbeat for edgeId "+edgeId);
							edgeHeartbeatHashMap.remove(edgeId);
							edgeHeartbeatsMissedHashMap.remove(edgeId);
						}

					}
										
					try {
						Thread.sleep(Constants.HEARTBEAT_CHECK_INTERVAL);
					} catch (InterruptedException e) {					
						e.printStackTrace();
					}
				} /** End of while loop **/
				
			}
		};
		
		Thread hbThread = new Thread(hbRunnable);
		hbThread.start();
		
		Runnable recoveryRunnable = new Runnable() {

			@Override
			public void run() {
				
				while(true) {
					startRecovery();
					
					try {
						Thread.sleep(Constants.RECOVERY_INTERVAL);
					} catch (InterruptedException e) {					
						e.printStackTrace();
					}
				}
				
			}
			
		};
		
		Thread recoveryThread = new Thread(recoveryRunnable);
		recoveryThread.start();
	}
	
	
	private void startRecovery() {
		
		HashMap<Integer, Integer> blocksLostHashMap = new HashMap<Integer, Integer>();
		ArrayList<DataNodeLocation> availableEdges = new ArrayList<DataNodeLocation>();
		ArrayList<DataNodeLocation> deadEdges = new ArrayList<DataNodeLocation>();
		
		if(edgesToRecover.size()==0) {
			LOGGER.info(" All edges are alive nothing to recover ");/** Very important condition **/
			return;
		}
		
		LOGGER.info("Recovery started ");
		Long startTime = System.currentTimeMillis();
		
		
		for(DataNodeLocation myDataLoc : edgeDevicesList.values()) {
			
			if(edgesToRecover.contains(myDataLoc.getNodeid())) {
				LOGGER.info("Adding to dead edges list "+myDataLoc.getNodeid());
				deadEdges.add(myDataLoc);
			}else {
				LOGGER.info("Adding to available edges list "+myDataLoc.getNodeid());
				availableEdges.add(myDataLoc);
			}
		}	
		
		for(DataNodeLocation myDataLoc : deadEdges) {
			
			for(Integer blockId : blockLocationsHashMap.keySet()) {
				
				ArrayList<DataNodeLocation> myDataLocs = blockLocationsHashMap.get(blockId);
				
				if(myDataLocs.contains(myDataLoc)) {
					if(blocksLostHashMap.containsKey(blockId)==false) {
						blocksLostHashMap.put(blockId,1);
						LOGGER.info("The value being put is for blockID "+blockId+" : 1");
					}else {
						Integer value = blocksLostHashMap.get(blockId);
						value = value + 1;
						blocksLostHashMap.put(blockId,value);
						LOGGER.info("The value being put is for blockID "+blockId+" : "+value);
					}
				}
			}
		}	
		
		HashMap<Integer, ArrayList<DataNodeLocation>> newBlocksHashMap = myRecovery.recover(blocksLostHashMap, blockLocationsHashMap, blockTypeHashMap, availableEdges, deadEdges);
		
		/**Crucial updates **/
		for(Integer blockId : newBlocksHashMap.keySet()) {
			ArrayList<DataNodeLocation> myDataLocs = newBlocksHashMap.get(blockId);
			LOGGER.info("Block id "+blockId+" The old blocks list "+blockLocationsHashMap.get(blockId));
			LOGGER.info("Block id "+blockId+" The new blocks list "+myDataLocs);
			
			blockLocationsHashMap.put(blockId, myDataLocs);
		}
		
		/**
		 * These structures need to be removed off of edge list
		 * edgeBlockListHashMap
			edgeDevicesList
			edgeHeartbeatHashMap
			edgeHeartbeatsMissedHashMap
		 */
		for(Integer edgeId : edgesToRecover) {
			edgeBlockListHashMap.remove(edgeId);
			edgeDevicesList.remove(edgeId);
			edgeHeartbeatHashMap.remove(edgeId);
			edgeHeartbeatsMissedHashMap.remove(edgeId);
		}
		
		for(DataNodeLocation myDataLoc : deadEdges) {
			boolean result = edgesToRecover.remove(new Integer(myDataLoc.getNodeid()));
			LOGGER.info("The element we tried to remove "+myDataLoc.getNodeid());
			LOGGER.info("edgesToRecover contains that element? "+edgesToRecover.contains(new Integer(myDataLoc.getNodeid())));
		}
		
		Long endTime = System.currentTimeMillis();
		LOGGER.info("erasure_coding_recovery_total="+blocksLostHashMap.size()+","+(endTime-startTime));
	}
	

	/**
	 * The session handle is returned by this method This is the first method that
	 * is called by the client when it wasnts to put a file
	 */
	public OpenFileResponse openFile(OpenFileRequest req) throws TException {

		String filename = req.getFilename();
		int filesize = req.getFilesize();
		double storageBudget = req.getStoragebudget();

		LOGGER.info("The filename is " + filename + " the filesize is " + filesize + " storagebudget " + storageBudget);

		Random random = new Random();
		Integer handle = random.nextInt(100000);

		populateStructures(filename, handle, filesize, storageBudget);

		/** only 2 fields **/
		OpenFileResponse response = new OpenFileResponse();
		response.setHandle(handle);
		response.setStatus(Constants.SUCCESS);

		return response;
	}

	/** Populate relevant structures **/
	public void populateStructures(String filename, Integer handle, int filesize, double storageBudget) {

		fileSessionHashMap.put(handle, filename);

		double numBlocks = filesize / (1.0 * Constants.BLOCKSIZE);
		double integerPart = (int) Math.floor(numBlocks);
		double fractionPart = numBlocks - integerPart;

		int fullBlocks = 0, partialBlocks = 0;

		if (fractionPart == 0.0) {
			fullBlocks = (int) integerPart;
		} else {
			fullBlocks = (int) integerPart;
			partialBlocks = 1;
		}

		int totalBlocks = fullBlocks + partialBlocks;
		LOGGER.info("Total blocks " + totalBlocks + " full blocks " + fullBlocks + " partial blocks " + partialBlocks);
		double tempVal = (2 / 3.0) * (double) storageBudget * (double) totalBlocks;
		LOGGER.info("The value is " + tempVal + "   - " + totalBlocks);

		int replicatedBlocks = 0, erasureCodedBlocks = 0;
		replicatedBlocks = (int) Math.floor((2 / 3.0) * (double) storageBudget * (double) totalBlocks - totalBlocks);
		erasureCodedBlocks = totalBlocks - replicatedBlocks;

		LOGGER.info("Erasure coded blocks = " + erasureCodedBlocks + " : Replicated Blocks = " + replicatedBlocks);
		replicatedBlockHashMap.put(handle, replicatedBlocks);
		fileBlockHashMap.put(filename, new ArrayList<Integer>());

	}

	/**
	 * This is a synchronized because it needs to maintain block numbers consistent
	 * over many parallel clients This is the only place where it is modified
	 * 
	 * @return a global block number
	 */
	public synchronized static Integer allotBlockNumber() {
		globalBlockNum = globalBlockNum + 1;
		return globalBlockNum;
	}

	public WriteBlockResponse requestBlocksToWrite(WriteBlockRequest writeBlockReq) throws TException {
		
		MyParser parser = new MyParser();
		HashMap<String,String> codingMap = parser.returnErasureCoding();
		Integer NUM_ERASURE_CODING = 6;
		Integer PARITY_SHARDS = 2;
		Integer DATA_SHARDS = 4;
		
		String num_coding = "6";
		String parity_shards = "2";
		String data_shards = "4";
		
		if(codingMap!=null) {
			num_coding = codingMap.get("total");
			parity_shards = codingMap.get("parity");
			data_shards = codingMap.get("data");
			
			NUM_ERASURE_CODING = Integer.parseInt(num_coding);
			PARITY_SHARDS = Integer.parseInt(parity_shards);
			DATA_SHARDS = Integer.parseInt(data_shards);
			
			LOGGER.info("The number of erasure coding shards "+NUM_ERASURE_CODING+" data "+DATA_SHARDS+" parity shards "+PARITY_SHARDS);
		}

		Integer handle = writeBlockReq.getHandle();
		WriteBlockResponse response = new WriteBlockResponse();
		Integer type = 0;
		Integer blockNumber = 0;
		ArrayList<DataNodeLocation> myNodeLocations = new ArrayList<DataNodeLocation>();

		if (replicatedBlockHashMap.get(handle) != null) {

			Integer replicatedBlockCount = replicatedBlockHashMap.get(handle);
			if (replicatedBlockCount != 0) { /** The case for replication **/

				LOGGER.info("The replicated block is" + replicatedBlockCount);

				replicatedBlockHashMap.put(handle, replicatedBlockCount - 1);/** This is an important step **/

				/** Allocate 3 blocks **/
				HashSet<Integer> myHashSet = new HashSet<Integer>();
				int numRand = edgeDevicesList.size(), count = 0;
				type = Constants.REPLICATION;

				while (count != Constants.NUM_REPLICATION) { /** Important field **/

					Random rand = new Random();
					int index = rand.nextInt(numRand);

					if (myHashSet.contains(index) == false) {
						count++; /** Incremented count **/
						myHashSet.add(index);

						DataNodeLocation myLoc = (new ArrayList<DataNodeLocation>(edgeDevicesList.values())).get(index); /** Sheshadri has made a change here **/
						LOGGER.info("Case Replication : Added a location " + myLoc.getIp() + " : " + myLoc.getPort());
						myNodeLocations.add(myLoc);
					}

				} /** End of while loop **/
			} else { /** The case for erasure coding **/

				/** Allocate 6 blocks **/
				HashSet<Integer> myHashSet = new HashSet<Integer>();
				int numRand = edgeDevicesList.size(), count = 0;
				type = Constants.ERASURE_CODING; /** Important field **/

				while (count != NUM_ERASURE_CODING) {

					Random rand = new Random();
					int index = rand.nextInt(numRand);

					if (myHashSet.contains(index) == false) {
						count++; /** Incremented count **/
						myHashSet.add(index);

						DataNodeLocation myLoc = (new ArrayList<DataNodeLocation>(edgeDevicesList.values())).get(index); /** Sheshadri has made a change here **/
						LOGGER.info(
								"Case Erasure Coding : Added a location " + myLoc.getIp() + " : " + myLoc.getPort());
						myNodeLocations.add(myLoc);
					}

				} /** End of while loop **/

			}
		} /** End of if **/

		blockNumber = allotBlockNumber();

		blockTypeHashMap.put(blockNumber, type);
		fileBlockHashMap.get(fileSessionHashMap.get(handle)).add(blockNumber); /** File name to number of blocks **/

		for (DataNodeLocation dnloc : myNodeLocations) {

			if (edgeBlockListHashMap.containsKey(dnloc.getNodeid()) == false) {/** FIrst time entrant **/

				ArrayList<Integer> blockList = new ArrayList<Integer>();
				blockList.add(blockNumber);
				edgeBlockListHashMap.put(dnloc.getNodeid(), blockList);

			} else {

				edgeBlockListHashMap.get(dnloc.getNodeid()).add(blockNumber); /** Append to the block list **/
			}
		}

		LOGGER.info("The edgeBlockListHashMap is " + edgeBlockListHashMap.toString());

		blockLocationsHashMap.put(blockNumber, myNodeLocations); /** THIS IS IMPORTANT WHILE READING **/

		response.setType(type);
		response.setBlocknum(blockNumber);
		response.setLocations(myNodeLocations);

		return response;
	}

	/**
	 * Returns block locations and type of the blocks (replicated or erasure coded )
	 */
	public ReadBlockResponse requestBlocksToRead(ReadBlockRequest readBlockReq) throws TException {

		String filename = readBlockReq.getFilename();
		ReadBlockResponse response = new ReadBlockResponse();
		ArrayList<BlockLocation> myBlockLocations = new ArrayList<BlockLocation>();

		LOGGER.info("Got request to Read " + readBlockReq.getFilename());
		LOGGER.info("File block hash map " + fileBlockHashMap.toString());

		response.setStatus(Constants.FAILURE);

		if (fileBlockHashMap.get(filename) != null) {
			ArrayList<Integer> fileBlockNums = fileBlockHashMap.get(filename);

			for (Integer blockNum : fileBlockNums) {

				BlockLocation myBlockLoc = new BlockLocation();
				Integer type = 0;
				ArrayList<DataNodeLocation> myDataNodeLocations = null;

				if (blockTypeHashMap.get(blockNum) != null) {
					type = blockTypeHashMap.get(blockNum); /** This is very important **/
					LOGGER.info("The block number being read is " + blockNum + " The type is " + type);
				}

				if (blockLocationsHashMap.get(blockNum) != null) {
					myDataNodeLocations = blockLocationsHashMap.get(blockNum); /** Another impornat field **/
				}

				myBlockLoc.setBlocknumber(blockNum);
				myBlockLoc.setLocations(myDataNodeLocations);
				myBlockLoc.setType(type);

				myBlockLocations.add(myBlockLoc); /** Block locations for the requeste file **/
			}

			response.setStatus(Constants.SUCCESS);
		}

		LOGGER.info("The number of block locations being sent is " + myBlockLocations.size());
		response.setBlocklocations(myBlockLocations);

		return response;
	}

	public ListFileResponse listFiles(ListFileRequest listFileReq) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	public BlockReportResponse blockReport(BlockReportRequest blockReportReq) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	/** Heartbeat request and response **/
	public HeartBeatResponse heartbeat(HeartBeatRequest heartBeatReq) throws TException {

		LOGGER.info("Got the heartbeat from edge " + heartBeatReq.getNodeId()+" : "+System.currentTimeMillis());
		HeartBeatResponse resp = new HeartBeatResponse();
		resp.setStatus(Constants.HEALTHY);
		
		edgeHeartbeatHashMap.put(heartBeatReq.getNodeId(), System.currentTimeMillis());
		
		return resp;
	}

	/**
	 * Remove the file session handle
	 */
	public CloseFileResponse closeFile(CloseFileRequest req) throws TException {

		fileSessionHashMap.remove(req.getHandle());
		
		CloseFileResponse response = new CloseFileResponse(Constants.SUCCESS);
		return response;
	}

	@Override
	public RegisterEdgeResponse registerEdge(RegisterEdgeRequest registerReq) throws TException {

		LOGGER.info("The incoming register request is " + registerReq.getNodeId() + " The ip and port is "
				+ registerReq.getEdgeLocation().getIp() + " : " + registerReq.getEdgeLocation().getPort());

		edgeDevicesList.put(registerReq.getNodeId(), registerReq.getEdgeLocation());
		
		Integer edgeId = registerReq.getNodeId();
		
		/** Set the heartbeat value to 0,  their aliveness or deadness will be checked by a separate thread **/
		edgeHeartbeatHashMap.put(edgeId, System.currentTimeMillis());
		
		/** Initially no heartbeats are missed **/
		edgeHeartbeatsMissedHashMap.put(edgeId, 0);

		LOGGER.info("The number of devices are " + edgeDevicesList.size());

		RegisterEdgeResponse response = new RegisterEdgeResponse(Constants.SUCCESS);

		return response;
	}

	@Override
	public CloseFileResponse clearStructures() throws TException {

		fileSessionHashMap.clear();
		fileBlockHashMap.clear();
		blockLocationsHashMap.clear();
		blockTypeHashMap.clear();
		replicatedBlockHashMap.clear();
		edgeBlockListHashMap.clear();
		edgeDevicesList.clear();
		edgeHeartbeatHashMap.clear();
		edgeHeartbeatsMissedHashMap.clear();
		edgesToRecover.clear();
		
		CloseFileResponse response = new CloseFileResponse(Constants.SUCCESS);
		return response;
	}

}
