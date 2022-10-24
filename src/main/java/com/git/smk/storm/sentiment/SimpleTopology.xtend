package com.git.smk.storm.sentiment

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import com.git.smk.storm.AggregatingBolt
import com.git.smk.storm.FileWritingBolt
import com.git.smk.storm.FilteringBolt
import com.git.smk.storm.RandomNumberSpout

class SimpleTopology {
	def static void main(String[] args) {
		var TopologyBuilder builder = new TopologyBuilder()
		builder.setSpout("quoteSpout", new SentimentDataSpout())
		builder.setBolt("sentimentBolt", new SentimentAnalysisBolt()).shuffleGrouping("quoteSpout")
		builder.setBolt("brandRecognizingBolt", new BrandRecognizingBolt()).shuffleGrouping("sentimentBolt")
		builder.setBolt("aggregatorBolt",
			new AggregatorBolt().withTimestampField("timestamp").withLag(BaseWindowedBolt.Duration.seconds(1)).
				withWindow(BaseWindowedBolt.Duration.seconds(20))).shuffleGrouping("brandRecognizingBolt")
		builder.setBolt("printingBolt", new PrintingBolt()).shuffleGrouping("aggregatorBolt")
		var Config config = new Config()
		config.setDebug(false)
		var LocalCluster cluster = new LocalCluster()
		cluster.submitTopology("Test", config, builder.createTopology())
	}

	def static void runTopology() {
		var String filePath = "./src/main/resources/operations.txt"
		var TopologyBuilder builder = new TopologyBuilder()
		builder.setSpout("randomNumberSpout", new RandomNumberSpout())
		builder.setBolt("filteringBolt", new FilteringBolt()).shuffleGrouping("randomNumberSpout")
		builder.setBolt("aggregatingBolt",
			new AggregatingBolt().withTimestampField("timestamp").withLag(BaseWindowedBolt.Duration.seconds(1)).
				withWindow(BaseWindowedBolt.Duration.seconds(5))).shuffleGrouping("filteringBolt")
		builder.setBolt("fileBolt", new FileWritingBolt(filePath)).shuffleGrouping("aggregatingBolt")
		var Config config = new Config()
		config.setDebug(false)
		var LocalCluster cluster = new LocalCluster()
		cluster.submitTopology("Test", config, builder.createTopology())
	}
}
