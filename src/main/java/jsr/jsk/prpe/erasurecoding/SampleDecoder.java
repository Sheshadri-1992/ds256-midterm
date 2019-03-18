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

	public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = 6;
    public static final int BYTES_IN_INT = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDecoder.class);

    @SuppressWarnings("ResultOfMethodCallIgnored")
    /**
     * 
     * @param filename i
     * @param blockNumber
     * @param myDataNodeLoc
     * @throws IOException
     */
    public void decode(String filename, int blockNumber ,ArrayList<DataNodeLocation> myDataNodeLoc)throws IOException {

//        final File originalFile = new File(filename);
//        if (!originalFile.exists()) {
//            LOGGER.info("Cannot read input file: " + originalFile);
//            return;
//        }
        
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
				FileOutputStream myOutStream = new FileOutputStream(new File(blockNum));
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
            		blockNumber + ":" + i); /** This is important **/
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
        LOGGER.info("Amount of bytes written "+BYTES_IN_INT);
        out.write(allBytes, BYTES_IN_INT, fileSize);
        LOGGER.info("Wrote " + decodedFile);
    }
}
