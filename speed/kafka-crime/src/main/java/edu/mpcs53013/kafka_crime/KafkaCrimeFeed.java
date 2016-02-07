package edu.mpcs53013.kafka_crime;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.ZipEntry;

import org.apache.commons.io.IOUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaCrimeFeed {
	static class Task extends TimerTask {
		Task() throws MalformedURLException {
			crimeURL = new URL("https://data.cityofchicago.org/api/views/t4d9-p93z/rows.csv?accessType=DOWNLOAD");
		}
		@Override
		public void run() {
			try {
				// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		        Properties props = new Properties();
//		        props.put("metadata.broker.list", "sandbox.hortonworks.com:6667");
//		        props.put("zk.connect", "localhost:2181");
		        props.put("metadata.broker.list", "hadoop-m.c.mpcs53013-2015.internal:6667");
		        props.put("zk.connect", "hadoop-w-1.c.mpcs53013-2015.internal:2181,hadoop-w-0.c.mpcs53013-2015.internal:2181,hadoop-m.c.mpcs53013-2015.internal:2181");
		        props.put("serializer.class", "kafka.serializer.StringEncoder");
		        props.put("request.required.acks", "1");

		        String TOPIC = "acidreflux-crime-events";
		        ProducerConfig config = new ProducerConfig(props);
		        InputStream iS = crimeURL.openStream();
		        
		        Producer<String, String> producer = new Producer<String, String>(config);
				BufferedReader br = new BufferedReader(new InputStreamReader(iS));
				
				//skip the header line
				String line = br.readLine();
				line = br.readLine();
				String[] tokens = line.split("[ ,]");
				String thisDate = tokens[1];
				
				
				do{
					StringWriter writer = new StringWriter();				
	                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, line);
	                producer.send(data);
					line = br.readLine();
					tokens = line.split("[ ,]");
				}while(tokens[1].compareTo(thisDate) == 0);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		URL crimeURL;
		
		
	}
	public static void main(String[] args) {
		try {
			Timer timer = new Timer();
			timer.scheduleAtFixedRate(new Task(), 0, 24*60*60*1000);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
