package jsr.jsk.prpe.miscl;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyParser {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(MyParser.class);

	public String returnJSON() {		
		
		String json = "";	
		
		try {
			File file = new File("conf.txt");
			FileInputStream fis = new FileInputStream(file);
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			fis.close();
			
			String str = new String(data, "UTF-8");
			json = str;
			System.out.println("The whole file is "+str);
		}catch(Exception e) {
			e.printStackTrace();
		}	
		
		return json;
	}
	
	public HashMap<String, String> returnMasterLocation(){
		
		JSONObject myObject = null;
		String json = returnJSON();
		HashMap<String, String> myDict = new HashMap<String, String>();
		
		try {
			myObject = new JSONObject(json);
			myDict.put("ip", myObject.getString("ip"));
			myDict.put("port",myObject.getString("port"));
			LOGGER.info("The dictionary is "+myDict.toString());
			
			
		} catch (JSONException e) {

			e.printStackTrace();
		}
		
		
		return myDict;
	}
	
	public static void main(String[] args) {
		MyParser myparser = new MyParser();
		myparser.returnMasterLocation();
	}
	
}