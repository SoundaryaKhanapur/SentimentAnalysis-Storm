package com.git.smk.storm.sentiment;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PrintingBolt extends BaseBasicBolt {
    
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("\nTrending brand: "+ (tuple.getValueByField("trendingBrand")) + "\nTrending Period => "+tuple.getValueByField("beginningTimestamp")+ " : " + tuple.getValueByField("endTimestamp"));

        
        
        String brand = "" + tuple.getValueByField("trendingBrand") + " : ";
        String period = "" + tuple.getValueByField("beginningTimestamp") +" : "+ tuple.getValueByField("endTimestamp");
        
        Values values = new Values(brand, period);
        basicOutputCollector.emit(values);
		
        
    }

   
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    //	outputFieldsDeclarer.declare(new Fields("trendingBrand", "timeRange"));
    	
    }
}
