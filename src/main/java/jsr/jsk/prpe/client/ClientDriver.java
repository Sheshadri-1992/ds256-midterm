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

		for (int i = 0; i < 100; i++) {
			filenames[i] = "microbatch" + i + ".txt";
		}

		for (int i = 0; i < 1; i++) {
			if (choice == 0) { /** Put Request **/

				LOGGER.info("The choice is put ");
				PutRequestClass myPutReq = new PutRequestClass(filenames[i], storageBudget);
				myPutReq.openRequest();
				myPutReq.putFile(); /** This would've ensured a session handle **/
				myPutReq.closeRequest();

			} else { /** Get request **/
				LOGGER.info("zxcqq READ Request started " + storageBudget);
				LOGGER.info("The choice is get ");
				GetRequestClass myGetReq = new GetRequestClass(filenames[i]);
				myGetReq.getFileReq();
				LOGGER.info("zxcqq READ Request ended " + storageBudget);
			}
		}
		
		LOGGER.info("zxcqq WRITE/READ Request ended ");

		long end = System.currentTimeMillis();
		long time = (end - start);

		String row = storageBudget + "," + time + "," + choice;
		LOGGER.info("krishna " + row);
		LOGGER.info("Time taken in sec " + time / 1000 + " Time taken in milli seconds " + time);
		try {
			myStream.write(row.getBytes());
			myStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
