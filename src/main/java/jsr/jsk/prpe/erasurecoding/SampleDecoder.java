/**
 * Command-line program that decodes a file using Reed-Solomon 4+2.
 *
 * Copyright 2015, Backblaze, Inc.  All rights reserved.
 */

package jsr.jsk.prpe.erasurecoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

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
import jsr.jsk.prpe.miscl.MyParser;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.GetRequest;
import jsr.jsk.prpe.thrift.GetResponse;

/**
 * Command-line program that decodes a file using Reed-Solomon 4+2.
 *
 * The file name given should be the name of the file to decode, say
 * "foo.txt".  This program will expected to find "foo.txt.0" through
 * "foo.txt.5", with at most two missing.  It will then write
 * "foo.txt.decoded".
 */
public class SampleDecoder {

	public static int DATA_SHARDS = 4;
    public static int PARITY_SHARDS = 2;
    public static int TOTAL_SHARDS = 6;
    public static final int BYTES_IN_INT = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDecoder.class);
    public ArrayList<String> lostBlocks = null;
    
    public SampleDecoder() {
	
    	lostBlocks = new ArrayList<String>();
    	MyParser parser = new MyParser();
		HashMap<String,String> codingMap = parser.returnErasureCoding();
		
		String num_coding = "6";
		String parity_shards = "2";
		String data_shards = "4";
		
		if(codingMap!=null) {
			num_coding = codingMap.get("total");
			parity_shards = codingMap.get("parity");
			data_shards = codingMap.get("data");
			
			TOTAL_SHARDS = Integer.parseInt(num_coding);
			PARITY_SHARDS = Integer.parseInt(parity_shards);
			DATA_SHARDS = Integer.parseInt(data_shards);
			
			LOGGER.info("The number of erasure coding shards "+TOTAL_SHARDS+" data "+DATA_SHARDS+" parity shards "+PARITY_SHARDS);
		}
	}

    @SuppressWarnings("ResultOfMethodCallIgnored")
    /**
     * 
     * @param filename i
     * @param blockNumber
     * @param myDataNodeLoc
     * @throws IOException
     */
    public void decode(String filename, int blockNumber ,ArrayList<DataNodeLocation> myDataNodeLoc)throws IOException {

    	File myDir = new File(Constants.TEMP_DIR);
    	if(myDir.exists()==false) {
    		myDir.mkdir();
    		LOGGER.info("Created the directory "+Constants.TEMP_DIR);
    	}
        
        for(DataNodeLocation dataloc : myDataNodeLoc) {
        	
        	String IP = dataloc.getIp();
        	int port = dataloc.getPort();
        	
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
    		
    		GetRequest myReq = new GetRequest(blockNumber);    		
    		try {
				GetResponse myRes = myClient.get(myReq);
				String blockNum = myRes.getBlockNumber();
				byte[] data = myRes.getData();
				LOGGER.info("The block number sent is "+blockNum); /**Sheshadri this is important **/
				
				/**Write this to intermediate directory**/
				FileOutputStream myOutStream = new FileOutputStream(new File(Constants.TEMP_DIR+blockNum));
				myOutStream.write(data);
				myOutStream.close();

			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		transport.close();
        }
        
        
        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
        final byte [] [] shards = new byte [TOTAL_SHARDS] [];
        final boolean [] shardPresent = new boolean [TOTAL_SHARDS];
        int shardSize = 0;
        int shardCount = 0;
        for (int i = 0; i < TOTAL_SHARDS; i++) {
//        	LOGGER.info(originalFile.getParentFile()+                  originalFile.getName() + "." + i);
            File shardFile = new File(
            		Constants.TEMP_DIR+blockNumber + ":" + i); /** This is important **/
            if (shardFile.exists()) {
                shardSize = (int) shardFile.length();
                shards[i] = new byte [shardSize];
                shardPresent[i] = true;
                shardCount += 1;
                InputStream in = new FileInputStream(shardFile);
                in.read(shards[i], 0, shardSize);
                in.close();
                LOGGER.info("Read " + shardFile);
            }
        }

        // We need at least DATA_SHARDS to be able to reconstruct the file.
        if (shardCount < DATA_SHARDS) {
            LOGGER.info("Not enough shards present");
            return;
        }

        // Make empty buffers for the missing shards.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte [shardSize];
            }
        }

        // Use Reed-Solomon to fill in the missing shards
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        // Combine the data shards into one buffer for convenience.
        // (This is not efficient, but it is convenient.)
        byte [] allBytes = new byte [shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        // Extract the file length
        int fileSize = ByteBuffer.wrap(allBytes).getInt();

        // Write the decoded file
        File decodedFile = new File(Constants.OUTPUT_DIR+blockNumber + ".decoded");/** Sheshadri Edited this **/
        OutputStream out = new FileOutputStream(decodedFile);
        LOGGER.info("Amount of bytes written "+fileSize);
        out.write(allBytes, BYTES_IN_INT, fileSize);
        LOGGER.info("Wrote " + decodedFile);
        
    }
    
    /** This is for recovery **/
    public byte[] decodeRecovery(int blockNumber,
    		ArrayList<DataNodeLocation> myDataNodeLoc,
    		ArrayList<DataNodeLocation> deadEdges,
    		ArrayList<DataNodeLocation> newAllotedEdges
    		) throws IOException {

    	File myDir = new File(Constants.RECOVERY_DIR);
    	if(myDir.exists()==false) {
    		myDir.mkdir();
    		LOGGER.info("Recovery Created the directory "+Constants.RECOVERY_DIR);
    	}
        
        for(DataNodeLocation dataloc : myDataNodeLoc) {
        	
        	if(deadEdges.contains(dataloc)) {
        		LOGGER.info("Recovery Looking for Erasure coded block in dead edge, moving on...");
        		continue;
        	}
        	
        	String IP = dataloc.getIp();
        	int port = dataloc.getPort();
        	
        	TTransport transport = new TFramedTransport(new TSocket(IP, port));
    		try {
    			transport.open();
    		} catch (TTransportException e) {
    			transport.close();
    			LOGGER.error("Recovery Error opening connection to IP : {} and port : {}", IP, port);
    			e.printStackTrace();
    			return null;
    		}
    		
    		TProtocol protocol = new TBinaryProtocol(transport);
    		EdgeService.Client myClient = new EdgeService.Client(protocol);
    		
    		GetRequest myReq = new GetRequest(blockNumber);    		
    		try {
				GetResponse myRes = myClient.get(myReq);
				String blockNum = myRes.getBlockNumber();
				byte[] data = myRes.getData();
				LOGGER.info("Recovery The block number sent is "+blockNum); /**Sheshadri this is important **/
				
				/**Write this to intermediate directory**/
				FileOutputStream myOutStream = new FileOutputStream(new File(Constants.RECOVERY_DIR+blockNum));
				myOutStream.write(data);
				myOutStream.close();

			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		transport.close();
        }
        
        
        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
        final byte [] [] shards = new byte [TOTAL_SHARDS] [];
        final boolean [] shardPresent = new boolean [TOTAL_SHARDS];
        int shardSize = 0;
        int shardCount = 0;
        for (int i = 0; i < TOTAL_SHARDS; i++) {

            File shardFile = new File(
            		Constants.RECOVERY_DIR+blockNumber + ":" + i); /** This is important **/
            if (shardFile.exists()) {
                shardSize = (int) shardFile.length();
                shards[i] = new byte [shardSize];
                shardPresent[i] = true;
                shardCount += 1;
                InputStream in = new FileInputStream(shardFile);
                in.read(shards[i], 0, shardSize);
                in.close();
                LOGGER.info("Recovery Read " + shardFile);
            }else {
            	lostBlocks.add(blockNumber+ ":" + i); /** This is important **/
            	LOGGER.info("Recovery Lost erasure coded block added "+(blockNumber+ ":" + i) );
            }
        }

        // We need at least DATA_SHARDS to be able to reconstruct the file.
        if (shardCount < DATA_SHARDS) {
            LOGGER.info("Recovery Not enough shards present");
            return null;
        }

        // Make empty buffers for the missing shards.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte [shardSize];
            }
        }

        // Use Reed-Solomon to fill in the missing shards
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        // Combine the data shards into one buffer for convenience.
        // (This is not efficient, but it is convenient.)
        byte [] allBytes = new byte [shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        // Extract the file length
        int fileSize = ByteBuffer.wrap(allBytes).getInt();

        // Write the decoded file
        File decodedFile = new File(Constants.RECOVERY_DIR+blockNumber);/** Sheshadri Edited this **/
        OutputStream out = new FileOutputStream(decodedFile);
        
        out.write(allBytes, BYTES_IN_INT, fileSize);
        LOGGER.info("Recovery Amount of bytes written "+fileSize);
        LOGGER.info("Recovery Wrote " + decodedFile);
        
        
        int lengthOfBytesToRead = (int) decodedFile.length();
        FileInputStream myFileStream = new FileInputStream(new File(Constants.RECOVERY_DIR+blockNumber));
        byte[] outputBytes = new byte[lengthOfBytesToRead];
        myFileStream.read(outputBytes);
        
        LOGGER.info("SampleDecoder sending bytes.. "+outputBytes.length);
        
        return outputBytes;
    }
    
    /**
     * 
     * @return the losterasure coded blocks per block
     */
    public ArrayList<String> getLostBlocksList() {
    	return lostBlocks;
    }
}
