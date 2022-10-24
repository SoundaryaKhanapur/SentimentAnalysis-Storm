package com.git.smk.storm.sentiment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class FileWritingBoalt extends BaseBasicBolt{

	static final Path path = Paths.get("Result.txt");
	
	
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		
		String result = ""+ tuple.getStringByField("trendingBrand") + tuple.getStringByField("timeRange") + System.lineSeparator() ;
		
		
		try {
			
			Files.write(path, result.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
			
			
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
		
	}
	
	

}
