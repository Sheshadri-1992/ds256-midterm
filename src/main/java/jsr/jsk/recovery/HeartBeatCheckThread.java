package jsr.jsk.recovery;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.miscl.Constants;

public class HeartBeatCheckThread {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatCheckThread.class);
	private Map<Integer,Long> edgeHeartbeatHashMap;
	

	/**
	 * Update the heartbeat values of all the edges
	 */
	public ArrayList<Integer> updateHeartBeat(Map<Integer, Long> argEdgeHeartbeatHashMap) {
		edgeHeartbeatHashMap = argEdgeHeartbeatHashMap;
		LOGGER.info("update Heart beat thread called ");
		ArrayList<Integer> myEdgeList = new ArrayList<Integer>();
		
		if(edgeHeartbeatHashMap==null) {
			LOGGER.info("Edge Heartbeat hash map is null");
			return myEdgeList;
		}
		
		for(Integer edgeId : edgeHeartbeatHashMap.keySet()) {
			
			Long timestamp = edgeHeartbeatHashMap.get(edgeId);
			Long currentTimestamp = System.currentTimeMillis();
			
			Long difference = (currentTimestamp - timestamp)/Constants.HEARTBEAT_INTERVAL; /** To give the answer in seconds **/
			
			if(difference>= Constants.ALLOWED_HB_MISS) {
				myEdgeList.add(edgeId);
				LOGGER.info("The edge added is "+edgeId);
			}
			
		}
		
		return myEdgeList;
	}

}
