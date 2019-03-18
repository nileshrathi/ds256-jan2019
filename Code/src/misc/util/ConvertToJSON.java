package misc.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class ConvertToJSON {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		BufferedReader myInputReader = null;
		HashMap<Long, ArrayList<Long>> myHashMap = new HashMap<Long, ArrayList<Long>>();
		
		BufferedWriter myOutputWriter = null;
		
		Long startTime = System.currentTimeMillis();
		
		try {
			myInputReader = new BufferedReader( new FileReader(new File("/home/swamiji/cit-Patents.txt")) );
			myOutputWriter = new BufferedWriter(new FileWriter(new File("/home/swamiji/big_output.txt")));
			
//			myInputReader = new BufferedReader( new FileReader(new File("/home/swamiji/sample_input.txt")) );
//			myOutputWriter = new BufferedWriter(new FileWriter(new File("/home/swamiji/sample_output.txt")));
			
			String line = myInputReader.readLine();
			long index = 0;
			while(line!=null) {
				
				if(line.contains("#"))
					continue;
				
				index++;
				String[] vertices = line.split("\\s+");
				Long key = Long.valueOf(vertices[0]);
				Long value = Long.valueOf(vertices[1]);
					
				if(myHashMap.containsKey(key)==false) {
					ArrayList<Long> myArr = new ArrayList<Long>();
					myArr.add(value);
					
					myHashMap.put(key, myArr);
				}else {					
					myHashMap.get(key).add(value);
				}
				line = myInputReader.readLine();
				System.out.println("index is "+index);
				
			}
			System.out.println("File reading complete ");
			for(Long key : myHashMap.keySet()) {
				
				ArrayList<Long> values = myHashMap.get(key);
				
				StringBuilder myBuilder = new StringBuilder();
				myBuilder = myBuilder.append("["+key.toString()+",0, [");
				
				for(Long value : values ) {
					myBuilder.append( "["+value.toString()+",0],");
				}
				
				myBuilder.append("]]");
				
				myOutputWriter.write(myBuilder.toString()+"\n");
				
			}
			
			myInputReader.close();
			myOutputWriter.close();
			
			Long endTime = System.currentTimeMillis();
			
			Long duration = (endTime - startTime)/1000;
			
			System.out.println("The file writing is complete");
			System.out.println("The amount of time in seconds "+duration);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
