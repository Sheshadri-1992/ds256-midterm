package jsr.jsk.prpe.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

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
import jsr.jsk.prpe.thrift.CloseFileRequest;
import jsr.jsk.prpe.thrift.CloseFileResponse;
import jsr.jsk.prpe.thrift.MasterService;

/**
 * The following are the steps that need to be taken to put and get a file
 * OpenFileRequest WriteBlockRequest PutRequest CloseRequest
 * 
 * @author swamiji
 *
 */
public class ClientDriver {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClientDriver.class);

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.exit(0);
		}

		int choice = (int) Integer.valueOf(args[0]);
		int handle = 0;
		String filename = args[1];
		double storageBudget = Double.valueOf(args[2]);
		FileOutputStream myStream = null;
		try {
			myStream = new FileOutputStream(new File("outputfile.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		LOGGER.info("zxcqq WRITE/READ Request started " + storageBudget);
		long start = System.currentTimeMillis();
		
		String[] filenames = new String[100];
		
		for(int i=0;i<100;i++) {
			filenames[i] = "microbatch"+i+".txt";
		}
		
		for(int i=0;i<5;i++) {
			if (choice == 0) { /** Put Request **/			
				
				LOGGER.info("The choice is put ");
				PutRequestClass myPutReq = new PutRequestClass(filenames[i], storageBudget);
				myPutReq.putFile();
				handle = myPutReq.getSessionHandle();
				

			} else { /** Get request **/
				LOGGER.info("zxcqq READ Request started " + storageBudget);
				LOGGER.info("The choice is get ");
				GetRequestClass myGetReq = new GetRequestClass(filenames[i]);
				myGetReq.getFileReq();
				LOGGER.info("zxcqq READ Request ended " + storageBudget);
			}
			
			
		}
		
		try {
			myStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOGGER.info("zxcqq WRITE/READ Request ended ");
		
		long end = System.currentTimeMillis();
		
		long time = (end-start);
		
		String row = storageBudget+","+time+","+choice;
		LOGGER.info("krishna "+row);
		LOGGER.info("Time taken in sec "+time/1000+" Time taken in milli seconds "+time);
		try {
			myStream.write(row.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		

//		TTransport transport = new TFramedTransport(new TSocket(Constants.MASTER_IP, Constants.MASTER_PORT));
//		try {
//			transport.open();
//		} catch (TTransportException e) {
//			transport.close();
//			LOGGER.error("Error opening connection to Master IP : {} and port : {}", Constants.MASTER_IP,
//					Constants.MASTER_PORT);
//			e.printStackTrace();
//		}
//
//		TProtocol protocol = new TBinaryProtocol(transport);
//		MasterService.Client masterClient = new MasterService.Client(protocol);
//
//		LOGGER.info("Close block Request with master ");
//		try {
//			CloseFileResponse response = masterClient.closeFile(new CloseFileRequest(handle));
//			LOGGER.info("The response is " + response.getStatus());
//		} catch (TException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
