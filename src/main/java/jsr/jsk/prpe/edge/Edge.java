package jsr.jsk.prpe.edge;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.thrift.HeartBeatRequest;
import jsr.jsk.prpe.thrift.HeartBeatResponse;
import jsr.jsk.prpe.thrift.MasterService;

public class Edge {

	private int nodeId;
	private double reliability;
	private static final Logger LOGGER = LoggerFactory.getLogger(Edge.class);
	
	public Edge(int nodeId, double reliability) {
		super();
		this.nodeId = nodeId;
		this.reliability = reliability;
	}


	public int getNodeId() {
		return nodeId;
	}


	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}


	public double getReliability() {
		return reliability;
	}


	public void setReliability(double reliability) {
		this.reliability = reliability;
	}
	
	
	public void sendHeartBeat() {
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
		LOGGER.info("Sending hearbeat to master ");
		
		HeartBeatRequest myHbReq = new HeartBeatRequest();
		myHbReq.setNodeId(nodeId);
		
		try {
			HeartBeatResponse response = masterClient.heartbeat(myHbReq);
			if(response.getStatus()==Constants.HEALTHY) {
				System.out.println("Got response from Master : HEALTHY");
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
