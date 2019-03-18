package jsr.jsk.prpe.servicehandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.GetRequest;
import jsr.jsk.prpe.thrift.GetResponse;
import jsr.jsk.prpe.thrift.PutRequest;
import jsr.jsk.prpe.thrift.PutResponse;

public class EdgeServiceHandler implements EdgeService.Iface {

	private static final Logger LOGGER = LoggerFactory.getLogger(EdgeServiceHandler.class);
	private int nodeId;

	public EdgeServiceHandler(int argNodeId) {

		nodeId = argNodeId;
		File myDir = new File(Constants.EDGE_DIR + nodeId + "/");
		if (myDir.exists() == false) {
			boolean result = myDir.mkdir();
			LOGGER.info("Directory creation " + result);
		}
	}

	@Override
	public PutResponse put(PutRequest putReq) throws TException {

		Integer status = 0;
		FileOutputStream myFileStream = null;

		try {
			String filename = putReq.getBlocknumber() + "";
			myFileStream = new FileOutputStream(new File(Constants.EDGE_DIR + nodeId + "/" + filename));
			myFileStream.write(putReq.getData());
			status = Constants.SUCCESS;
			myFileStream.close();
		} catch (Exception e) {
			LOGGER.info("Put Request has failed ");
			e.printStackTrace();
			status = Constants.FAILURE;
		}

		PutResponse response = new PutResponse();
		response.setResponse(status);

		return response;
	}

	@Override
	public GetResponse get(GetRequest getReq) throws TException {

		File folder = new File(Constants.EDGE_DIR + nodeId + "/");
		File[] listOfFiles = folder.listFiles(); /** maybe this was the fix needed **/

		String blockNum = getReq.getBlockNumber() + "";
		GetResponse myResponse = new GetResponse();
		LOGGER.info("The blocknumber requested is " + blockNum);

		FileInputStream myStream = null;

		for (int i = 0; i < listOfFiles.length; i++) {

			String filename = listOfFiles[i].getName();

			if (listOfFiles[i].isFile()) {
				LOGGER.info("File " + listOfFiles[i].getName());

				/** File reading happening here **/
				if (filename.startsWith((blockNum + ":"))) {
					try {
						File myFile = new File(Constants.EDGE_DIR + nodeId + "/" + filename); /** This was a problem **/
						myStream = new FileInputStream(myFile);
						long size = myFile.length();
						byte[] data = new byte[(int) size];
						myStream.read(data);

						/** need to set 3 fields **/
						myResponse.setBlockNumber(filename);
						myResponse.setData(data);
						myResponse.setStatus(Constants.SUCCESS);
						myStream.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}else if (filename.equals(blockNum)) {/** case of replication **/

					File myFile = new File(Constants.EDGE_DIR + nodeId + "/" + filename); /** This was a problem **/
					try {
						myStream = new FileInputStream(myFile);
						long size = myFile.length();
						byte[] data = new byte[(int) size];
						myStream.read(data);

						/** need to set 3 fields **/
						myResponse.setBlockNumber(filename);
						myResponse.setData(data);
						myResponse.setStatus(Constants.SUCCESS);
						myStream.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			} else if (listOfFiles[i].isDirectory()) {
				LOGGER.info("Directory " + listOfFiles[i].getName());
			} 
		}

		return myResponse;
	}

}
