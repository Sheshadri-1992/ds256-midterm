/**
 * Command-line program encodes one file using Reed-Solomon 4+2.
 *
 * Copyright 2015, Backblaze, Inc.
 */

package jsr.jsk.prpe.erasurecoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

import jsr.jsk.prpe.erasurecoding.ReedSolomon;

import jsr.jsk.prpe.miscl.Constants;
import jsr.jsk.prpe.miscl.MyParser;
import jsr.jsk.prpe.thrift.DataNodeLocation;
import jsr.jsk.prpe.thrift.EdgeService;
import jsr.jsk.prpe.thrift.PutRequest;
import jsr.jsk.prpe.thrift.PutResponse;

/**
 * Command-line program encodes one file using Reed-Solomon 4+2.
 *
 * The one argument should be a file name, say "foo.txt". This program will
 * create six files in the same directory, breaking the input file into four
 * data shards, and two parity shards. The output files are called "foo.txt.0",
 * "foo.txt.1", ..., and "foo.txt.5". Numbers 4 and 5 are the parity shards.
 *
 * The data stored is the file size (four byte int), followed by the contents of
 * the file, and then padded to a multiple of four bytes with zeros. The padding
 * is because all four data shards must be the same size.
 */
public class SampleEncoder {
	
	class MyThread extends Thread {

		private EdgeService.Client edgeClient = null;
		private String outputFile = "";
		private byte[] shards = null;
		
		public MyThread(String s, EdgeService.Client myClient, String outputFile, byte[] shard) {
			super(s);
			
			edgeClient = myClient;
			this.outputFile = outputFile;
			shards = shard;
		}

		public void run() {
			System.out.println("Running Thread: " + getName());
			
			PutRequest myPutReq = new PutRequest(outputFile, ByteBuffer.wrap(shards)); /**THIS was the EPIC MISTAKE **/
            try {
                PutResponse myRes = edgeClient.put(myPutReq);
                if (myRes.getResponse() == Constants.SUCCESS) {
                    LOGGER.info("Erasure coded block successfully written ");                   
                }

            } catch (TException e) {

                e.printStackTrace();
            }
		}
	}

    public static int DATA_SHARDS = 4;
    public static int PARITY_SHARDS = 2;
    public static int TOTAL_SHARDS = 6;

    public static final int BYTES_IN_INT = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleEncoder.class);

    public SampleEncoder() {
        LOGGER.info("Sample Encoder object got created");
        
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

    public void encode(String filename, ArrayList<DataNodeLocation> myDataLoc, byte[] data) throws IOException {


        System.out.println("The number of shards are "+TOTAL_SHARDS);

        // Get the size of the input file.  (Files bigger that
        // Integer.MAX_VALUE will fail here!)
//        final int fileSize = (int) inputFile.length();
        final int fileSize = (int) data.length;

        // Figure out how big each shard will be.  The total size stored
        // will be the file size (8 bytes) plus the file.
        final int storedSize = fileSize + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the file size, followed by
        // the contents of the file.
        final int bufferSize = shardSize * DATA_SHARDS;
        final byte [] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(fileSize);
        
        
        /** InputStream in = new FileInputStream(inputFile);
        int bytesRead = in.read(allBytes, BYTES_IN_INT, fileSize);
        if (bytesRead != fileSize) {
            throw new IOException("not enough bytes read");
        }
        in.close(); **/ //Commented by Sheshadri 
        
        System.arraycopy(data, 0, allBytes, BYTES_IN_INT, fileSize); /** This is the change I made : Sheshadri **/

        // Make the buffers to hold the shards.
        byte [] [] shards = new byte [TOTAL_SHARDS] [shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity.
        Long startTime = System.currentTimeMillis();
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);
        Long endTime = System.currentTimeMillis();
        LOGGER.info("erasure_coding_code_time="+filename+","+(endTime-startTime));
        
        MyThread[] threads = new MyThread[TOTAL_SHARDS];
        
        startTime = System.currentTimeMillis();
        /** Multiple threads for Writing parallel **/
        for (int i = 0; i < TOTAL_SHARDS; i++) {

            String outputFile = filename + ":" + i;
            String IP = myDataLoc.get(i).getIp();
            int port = myDataLoc.get(i).getPort();

            TTransport transport = new TFramedTransport(new TSocket(IP, port));
            try {
                transport.open();
            } catch (TTransportException e) {
                transport.close();
                LOGGER.error("Error opening connection to IP : {} and port : {}", IP, port);
                e.printStackTrace();
                return;
            }

            TProtocol protocol = new TBinaryProtocol(transport);
            EdgeService.Client myClient = new EdgeService.Client(protocol);
            
            threads[i] = new MyThread("Thread #" + i,myClient,outputFile,shards[i]);
            threads[i].start();
            

        }
        
        /**Important part **/
        for(int i=0;i< TOTAL_SHARDS;i++) {
        	try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        endTime = System.currentTimeMillis();
        LOGGER.info("erasure_coding_io_time="+filename+","+(endTime-startTime));
    }
    
    
    public void encodeRecovery(String filename, ArrayList<String> lostBlocks ,ArrayList<DataNodeLocation> myDataLoc, byte[] data) throws IOException {

    	LOGGER.info("Entered the recovery encoded method in Erasure coding");
    	LOGGER.info("Size of data being sent is "+data.length);
    	// Get the size of the input file.  (Files bigger that
        // Integer.MAX_VALUE will fail here!)
//        final int fileSize = (int) inputFile.length();
        final int fileSize = (int) data.length;

        // Figure out how big each shard will be.  The total size stored
        // will be the file size (8 bytes) plus the file.
        final int storedSize = fileSize + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the file size, followed by
        // the contents of the file.
        final int bufferSize = shardSize * DATA_SHARDS;
        final byte [] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(fileSize);
        
        
        /** InputStream in = new FileInputStream(inputFile);
        int bytesRead = in.read(allBytes, BYTES_IN_INT, fileSize);
        if (bytesRead != fileSize) {
            throw new IOException("not enough bytes read");
        }
        in.close(); **/ //Commented by Sheshadri 
        
        System.arraycopy(data, 0, allBytes, BYTES_IN_INT, fileSize); /** This is the change I made : Sheshadri **/

        // Make the buffers to hold the shards.
        byte [] [] shards = new byte [TOTAL_SHARDS] [shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity.
        Long startTime = System.currentTimeMillis();        
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);
        Long endTime = System.currentTimeMillis();
        LOGGER.info("erasure_coding_code_time_recovery="+filename+","+(endTime-startTime));
        
        MyThread[] threads = new MyThread[TOTAL_SHARDS];
        
        LOGGER.info("The blocks that are lost are "+lostBlocks.toString());
        
        startTime = System.currentTimeMillis();
        /** Multiple threads for Writing parallel **/
        int recIndex = 0;
        for (int i = 0; i < TOTAL_SHARDS; i++) {
        	
        	String outputFile = filename + ":" + i;
        	
        	if(recIndex>=myDataLoc.size())
        		recIndex = 0;
                        
            String IP = myDataLoc.get(recIndex).getIp(); /**Errors can spring up here!! **/
            int port = myDataLoc.get(recIndex).getPort();
            
            TTransport transport = new TFramedTransport(new TSocket(IP, port));
            try {
                transport.open();
            } catch (TTransportException e) {
                transport.close();
                LOGGER.error("Recovery Error opening connection to Master IP : {} and port : {}", IP, port);
                e.printStackTrace();
                return;
            }

            TProtocol protocol = new TBinaryProtocol(transport);
            EdgeService.Client myClient = new EdgeService.Client(protocol);
            
            threads[i] = new MyThread("Thread #" + i,myClient,outputFile,shards[i]);
            
            if(lostBlocks.contains(outputFile)==false) { /** this is important **/
            	LOGGER.info("Recovery : avoiding already available put blocks ");
            	continue;
            }/** Maybe this was the error **/
            
            LOGGER.info("Recovery : Block will be recovered ");
            threads[i].start();
            
            if(recIndex<myDataLoc.size())
            	recIndex++; /**This is important **/
        }
        
        /**Important part **/
        for(int i=0;i< TOTAL_SHARDS;i++) {
        	try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        endTime = System.currentTimeMillis();
        LOGGER.info("erasure_coding_io_time_recovery="+filename+","+(endTime-startTime));
        LOGGER.info("Recovery Successfully written the lost erasure coded blocks ");

    }
}
