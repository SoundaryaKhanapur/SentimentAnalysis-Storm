package com.git.smk.storm.sentiment;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentimentDataSpout extends BaseRichSpout{
	
    private SpoutOutputCollector collector;
    static long i=0;
    int negmic = 0;
	int neggog = 0;
	 int papple= 0;
     int pmicro = 0;
     int pgoo = 0;
    static Random r = new Random(51);

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutCollector) {
		collector=spoutCollector;
	}

	@Override
	public void nextTuple() {
		
		
		//Scanner s = new Scanner(System.in);
		Utils.sleep(1000);
		String brand="";
		//System.out.println("Enter: ");
		//brand = s.nextLine();
		i=r.nextInt(5);
		if(i%5==1)
		{	brand="i like apple";
		//	System.out.println(brand);
		papple++;
		}
		else if(i%5==2)
		{	brand="i love microsoft";
		pmicro++;
			//System.out.println(brand);
		}
		else if(i%5==3)
		{	brand="i hate microsoft";
			negmic++;
			//System.out.println("Negative count for microsoft: " +negmic );
		}
		else if(i%5==4)
		{	brand="i hate google";
		neggog++;
		//System.out.println("Negative count for google: "  +neggog );
			//System.out.println(brand);
		}
		else
		{
			brand="i prefer google";
			pgoo++ ; 
			//System.out.println(brand);
		}
		Utils.sleep(500);
		if(negmic > 0)
			System.out.println("Negative count for microsoft: " + negmic );
		if(neggog > 0)
			System.out.println("Negative count for google: "  + neggog );
		
		System.out.println("Positive count for google: "  + pgoo  );
		System.out.println("Positive count for apple: "  + papple );
		System.out.println("Positive count for microsoft: "  + pmicro );
		
		long timestamp = System.currentTimeMillis();
		System.out.println("Emitting: "+brand+" "+new Date(timestamp));
        Values values = new Values(brand, timestamp);
        collector.emit(values);
        
       
	}
	
	 
		

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("quote", "timestamp"));
		
	}

}
