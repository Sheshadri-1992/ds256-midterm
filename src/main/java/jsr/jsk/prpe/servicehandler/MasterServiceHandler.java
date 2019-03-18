package jsr.jsk.prpe.servicehandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.miscl.Constants;
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

public class MasterServiceHandler implements MasterService.Iface{
	
	public static HashMap<Integer, String> fileSessionHashMap; /** Filename is sessionHandle **/
	public static HashMap<String, ArrayList<Integer>> fileBlockHashMap; /** File and set of blocks **/
	public static HashMap<Integer, ArrayList<DataNodeLocation>> blockLocationsHashMap; /** Block number and List of edge locations **/
	public static HashMap<Integer, Integer> blockTypeHashMap; /** Holds a mapping between block number and type **/
	public static HashMap<Integer, Integer> replicatedBlockHashMap; /** tells per session how many replicated blocks are to be awarded **/
	public static HashMap<Integer, List<Integer>> edgeBlockListHashMap;/** Edge and the blocks they hold **/
	public static List<DataNodeLocation> edgeDevicesList;
	
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
		edgeDevicesList = new ArrayList<DataNodeLocation>();
	}

	
	
	public OpenFileResponse openFile(OpenFileRequest req) throws TException {
		
		String filename = req.getFilename();
		int filesize = req.getFilesize();
		double storageBudget = req.getStoragebudget();
		
		LOGGER.info("The filename is "+filename+" the filesize is "+filesize+ " storagebudget "+storageBudget);
		
		Random random = new Random();
		Integer handle  = random.nextInt(100000);
		
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
		
		int totalBlocks = fullBlocks + partialBlocks;
		LOGGER.info("Total blocks "+totalBlocks+" full blocks "+fullBlocks+ " partial blocks "+partialBlocks);
		double tempVal = (2/3.0)*(double)storageBudget*(double)totalBlocks;
		LOGGER.info("The value is "+tempVal+"   - "+totalBlocks);
		
		int replicatedBlocks = 0, erasureCodedBlocks = 0;		
		replicatedBlocks = (int) Math.floor( (2/3.0)*(double)storageBudget*(double)totalBlocks - totalBlocks ) ;		
		erasureCodedBlocks = totalBlocks - replicatedBlocks;
		
		LOGGER.info("Erasure coded blocks = "+erasureCodedBlocks+" : Replicated Blocks = "+replicatedBlocks);
		replicatedBlockHashMap.put(handle, replicatedBlocks);
		fileBlockHashMap.put(filename, new ArrayList<Integer>());
		
	}
	
	/**
	 * This is a synchronized because it needs to maintain block numbers consistent over many parallel clients
	 * This is the only place where it is modified
	 * @return a global block number 
	 */
	public synchronized static  Integer allotBlockNumber() {
		globalBlockNum = globalBlockNum + 1;
		return globalBlockNum;
	}

	public WriteBlockResponse requestBlocksToWrite(WriteBlockRequest writeBlockReq) throws TException {

		Integer handle = writeBlockReq.getHandle();
		WriteBlockResponse response = new WriteBlockResponse();
		Integer type = 0;
		Integer blockNumber = 0;
		ArrayList<DataNodeLocation> myNodeLocations = new ArrayList<DataNodeLocation>();
		
		if(replicatedBlockHashMap.get(handle)!=null) {
			
			Integer replicatedBlockCount = replicatedBlockHashMap.get(handle);
			if(replicatedBlockCount!=0) { /** The case for replication **/
				
				LOGGER.info("The replicated block is"+replicatedBlockCount);
				
				replicatedBlockHashMap.put(handle, replicatedBlockCount-1);/** This is an important step **/
				
				/** Allocate 3 blocks **/
				HashSet<Integer> myHashSet = new HashSet<Integer>();				
				int numRand = edgeDevicesList.size(), count = 0;
				type = Constants.REPLICATION;
				
				while(count!=Constants.NUM_REPLICATION) { /** Important field **/
					
					Random rand = new Random();
					int index = rand.nextInt(numRand);
					
					if(myHashSet.contains(index)==false) {
						count++; /** Incremented count **/
						myHashSet.add(index);
						
						DataNodeLocation myLoc = edgeDevicesList.get(index);
						LOGGER.info("Case Replication : Added a location "+myLoc.getIp()+" : "+myLoc.getPort());
						myNodeLocations.add(myLoc);
					}
					
				}/** End of while loop **/				
			}
			else { /** The case for erasure coding **/
				
				/** Allocate 6 blocks **/
				HashSet<Integer> myHashSet = new HashSet<Integer>();				
				int numRand = edgeDevicesList.size(), count = 0;
				type = Constants.ERASURE_CODING; /** Important field **/
				
				while(count!=Constants.NUM_ERASURE_CODING) {
					
					Random rand = new Random();
					int index = rand.nextInt(numRand);
					
					if(myHashSet.contains(index)==false) {
						count++; /** Incremented count **/
						myHashSet.add(index);
						
						DataNodeLocation myLoc = edgeDevicesList.get(index);
						LOGGER.info("Case Erasure Coding : Added a location "+myLoc.getIp()+" : "+myLoc.getPort());
						myNodeLocations.add(myLoc);
					}
					
				}/** End of while loop **/
				
			}
		}/** End of if **/
		
		blockNumber = allotBlockNumber();
		
		blockTypeHashMap.put(blockNumber,type);
		fileBlockHashMap.get(fileSessionHashMap.get(handle)).add(blockNumber); /** File name to number of blocks **/ 
		
		for(DataNodeLocation dnloc : myNodeLocations) {
			
			if(edgeBlockListHashMap.containsKey(dnloc.getNodeid())==false) {/** FIrst time entrant **/
				
				ArrayList<Integer> blockList = new ArrayList<Integer>();
				blockList.add(blockNumber);				
				edgeBlockListHashMap.put(dnloc.getNodeid(), blockList);
				
			}else {
				
				edgeBlockListHashMap.get(dnloc.getNodeid()).add(blockNumber); /** Append to the block list **/
			}
		}
		
		LOGGER.info("The edgeBlockListHashMap is "+edgeBlockListHashMap.toString());
		
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
		
		LOGGER.info("Got request to Read "+readBlockReq.getFilename());
		LOGGER.info("File block hash map "+fileBlockHashMap.toString());
		
		response.setStatus(Constants.FAILURE);
		
		if(fileBlockHashMap.get(filename)!=null) {
			ArrayList<Integer> fileBlockNums = fileBlockHashMap.get(filename);
			
			for(Integer blockNum : fileBlockNums) {
				
				BlockLocation myBlockLoc = new BlockLocation();
				Integer type= 0;
				ArrayList<DataNodeLocation> myDataNodeLocations = null;
				
				
				if(blockTypeHashMap.get(blockNum)!=null) {
					type = blockTypeHashMap.get(blockNum);	/** This is very important **/
					LOGGER.info("The block number being read is "+blockNum+" The type is "+type);
				}
				
				if(blockLocationsHashMap.get(blockNum)!=null) {
					myDataNodeLocations = blockLocationsHashMap.get(blockNum); /** Another impornat field **/
				}
				
				myBlockLoc.setBlocknumber(blockNum);
				myBlockLoc.setLocations(myDataNodeLocations);
				myBlockLoc.setType(type);
				
				myBlockLocations.add(myBlockLoc); /** Block locations for the requeste file **/
			}
			
			response.setStatus(Constants.SUCCESS);
		}
		
		LOGGER.info("The number of block locations being sent is "+myBlockLocations.size());
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

		LOGGER.info("Got the heartbeat from edge "+heartBeatReq.getNodeId());
		HeartBeatResponse resp = new HeartBeatResponse();
		resp.setStatus(Constants.HEALTHY);
		
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
		
		LOGGER.info("The incoming register request is "+registerReq.getNodeId()+" The ip and port is "+registerReq.getEdgeLocation().getIp()+ " : "+registerReq.getEdgeLocation().getPort() );
		
		edgeDevicesList.add(registerReq.getEdgeLocation());
		
		LOGGER.info("The number of devices are "+edgeDevicesList.size());
		
		RegisterEdgeResponse response = new RegisterEdgeResponse(Constants.SUCCESS);
		
		return response;
	}

}
