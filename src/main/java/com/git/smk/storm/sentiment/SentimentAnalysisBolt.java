package com.git.smk.storm.sentiment;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSampleStream;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

public class SentimentAnalysisBolt extends BaseBasicBolt {
	
    static DoccatModel model;
    static {
        InputStream dataIn = null;
        try {
            dataIn = new FileInputStream("./src/main/resources/TrainModelInput.txt");
            ObjectStream lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
            ObjectStream sampleStream = new DocumentSampleStream(lineStream);
            // Specifies the minimum number of times a feature must be seen
            int cutoff = 2;
            int trainingIterations = 30;
            model = DocumentCategorizerME.train("en", sampleStream, cutoff,
                    trainingIterations);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (dataIn != null) {
                try {
                    dataIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    static int positive = 0;
    static int negative = 0;

    public static void main(String[] args) throws IOException {
        String line = "";
        SentimentAnalysisBolt analyzer = new SentimentAnalysisBolt();
        analyzer.trainModel();
        int result1 = 0;

            result1 = analyzer.classifyNewQuote("i prefer apple");
            if (result1 == 1) {
                positive++;
            } else {
                negative++;
            }

    }

    public void trainModel() {
        InputStream dataIn = null;
        try {
            dataIn = new FileInputStream("./src/main/resources/TrainModelInput.txt");
            ObjectStream lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
            ObjectStream sampleStream = new DocumentSampleStream(lineStream);
            // Specifies the minimum number of times a feature must be seen
            int cutoff = 2;
            int trainingIterations = 30;
            model = DocumentCategorizerME.train("en", sampleStream, cutoff,
                    trainingIterations);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (dataIn != null) {
                try {
                    dataIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int classifyNewQuote(String quote) throws IOException {
        DocumentCategorizerME myCategorizer = new DocumentCategorizerME(model);
        double[] outcomes = myCategorizer.categorize(quote);
        String category = myCategorizer.getBestCategory(outcomes);
        
        //System.out.print("-----------------------------------------------------\nquote :" + quote + " ===> ");
        if (category.equalsIgnoreCase("1")) {
           // System.out.println(" POSITIVE ");
            return 1;
        } else {
        	
           // System.out.println(quote + " NEGATIVE ");
            
            return 0;
        }

    }

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String quote = tuple.getStringByField("quote");
		
        try {
        	
			if(classifyNewQuote(quote) > 0 ) {
				Values values = new Values("positive", tuple.getLongByField("timestamp"), tuple.getStringByField("quote"));
				collector.emit(values);
						}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentiment", "timestamp", "quote"));
		
	}
}

