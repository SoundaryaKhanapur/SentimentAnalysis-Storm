package com.git.smk.storm.sentiment;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;



public class SimpleTopology {
	
	public static void main(String[] args) {
	       
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("quoteSpout", new SentimentDataSpout());
		builder.setBolt("sentimentBolt", new SentimentAnalysisBolt()).shuffleGrouping("quoteSpout");
		builder.setBolt("brandRecognizingBolt", new BrandRecognizingBolt()).shuffleGrouping("sentimentBolt");
	
		 builder.setBolt("aggregatorBolt", new AggregatorBolt()
		          .withTimestampField("timestamp")
		          .withLag(BaseWindowedBolt.Duration.seconds(1))
		        //  .withWindow(BaseWindowedBolt.Duration.seconds(20))).shuffleGrouping("brandRecognizingBolt");
		          .withTumblingWindow(BaseWindowedBolt.Duration.seconds(20))
		          ).shuffleGrouping("brandRecognizingBolt");
		builder.setBolt("printingBolt", new PrintingBolt()).shuffleGrouping("aggregatorBolt");
	
	//	builder.setBolt("fileWritingBoalt", new FileWritingBoalt()).shuffleGrouping("printingBolt");
		
		 Config config = new Config();
	        config.setDebug(false);
	        config.setMessageTimeoutSecs(45);
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("Test", config, builder.createTopology());
	    }

	
}
