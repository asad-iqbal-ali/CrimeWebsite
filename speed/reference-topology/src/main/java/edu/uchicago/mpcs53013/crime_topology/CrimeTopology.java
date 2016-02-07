package edu.uchicago.mpcs53013.crime_topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class CrimeTopology {

	static class FilterReportsBolt extends BaseBasicBolt {
		//The Kafka topic contains crime reports in the same configuration
		//as the base data file, so the parser for those messages is the same
		//as the one from CrimeSummaryProcessor
		Pattern lineParser;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			lineParser = Pattern.compile("\"([^\"]*)\",|([^,]*),");
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			
			String report = tuple.getString(0);
			
			Matcher m = lineParser.matcher(report);
			
			//The fields we are interested in are the month, year, type of crime, and ward
			String[] datetime ={""};
			short ward = 0;
			String type = "";
			
			//scan through the line and grab the datetime array, type, and ward.
			//if the data does not contain the ward, discard it.
			for(int i = 0; i < 14; ++i){
				m.find();
				if(i == 1)
					datetime = m.group(2).split("[ :/]");
				else if(i == 4)
					type = m.group(2);
				else if(i == 10)
					try{
						ward = Short.parseShort(m.group(2));
					}catch (NumberFormatException e){return;}
			}
			//If any of the crucial data was missing, discard
			if(StringUtils.isEmpty(datetime[0]) || StringUtils.isEmpty(datetime[2]) || StringUtils.isEmpty(type))
				return;
			
			collector.emit(new Values(Byte.parseByte(datetime[0]), Short.parseShort(datetime[2]), type, ward));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("month", "year", "type", "ward"));
		}

	}


	static class UpdateCrimesBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private HConnection hConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
				conf.set("zookeeper.znode.parent", "/hbase-unsecure");
				hConnection = HConnectionManager.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			HTableInterface table = null;
			try {
				table = hConnection.getTable("acidreflux_crime_data");
				
				//get the key
				String key = input.getByteByField("month") + "-" + input.getShortByField("ward");
				
				String type = input.getStringByField("type");
				
				//Check to see if the year of the current input is higher than the max year 
				//of the row it's being added to
				Get getRow = new Get(Bytes.toBytes(key));
				Result crime = table.get(getRow);
				if(crime.isEmpty())
					return;
				byte[] yearVal = crime.getColumnLatestCell(Bytes.toBytes("year"), Bytes.toBytes("year")).getValueArray();
				short inputYear = input.getShortByField("year");
				byte[] inpYear = Bytes.toBytes(inputYear);
				
				//if so, increment that row's year by one
				if(!yearVal.equals(inpYear)){
					Increment inc = new Increment(Bytes.toBytes(key));
					inc.addColumn(Bytes.toBytes("year"), Bytes.toBytes("year"), 1);
					table.increment(inc);
				}
				
				//Match the type of the tuple to the row, and increment that row
				Increment increment = new Increment(Bytes.toBytes(key));
				
				if(type.equals("ARSON")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("ARSON"), 1);
				}
				if(type.equals("THEFT")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("THEFT"), 1);
				}
				if(type.equals("ASSAULT")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("ASSAULT"), 1);
				}
				if(type.equals("BATTERY")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("BATTERY"), 1);
				}
				if(type.equals("ROBBERY")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("ROBBERY"), 1);
				}
				if(type.equals("BURGLARY")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("BURGLARY"), 1);
				}
				if(type.equals("GAMBLING")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("GAMBLING"), 1);
				}
				if(type.equals("HOMICIDE")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("HOMICIDE"), 1);
				}
				if(type.equals("STALKING")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("STALKING"), 1);
				}
				if(type.equals("NARCOTICS")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("NARCOTICS"), 1);
				}
				if(type.equals("OBSCENITY")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("OBSCENITY"), 1);
				}
				if(type.equals("RITUALISM")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("RITUALISM"), 1);
				}
				if(type.equals("KIDNAPPING")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("KIDNAPPING"), 1);
				}
				if(type.equals("SEX OFFENSE")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("SEX_OFFENSE"), 1);
				}
				if(type.equals("INTIMIDATION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("INTIMIDATION"), 1);
				} 
				if(type.equals("NON - CRIMINAL") || type.equals("NON-CRIMINAL (SUBJECT SPECIFIED)") || type.equals("NON-CRIMINAL")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("NON_CRIMINAL"), 1);
				}
				if(type.equals("PROSTITUTION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("PROSTITUTION"), 1);
				}
				if(type.equals("OTHER OFFENSE")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("OTHER_OFFENSE"), 1);
				}
				if(type.equals("CRIMINAL DAMAGE")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("CRIMINAL_DAMAGE"), 1);
				}
				if(type.equals("PUBLIC INDECENCY")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("PUBLIC_INDECENCY"), 1);
				}
				if(type.equals("CRIMINAL TRESPASS")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("CRIMINAL_TRESPASS"), 1);
				}
				if(type.equals("DOMESTIC VIOLENCE")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("DOMESTIC_VIOLENCE"), 1);
				}
				if(type.equals("HUMAN TRAFFICKING")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("HUMAN_TRAFFICKING"), 1);
				}
				if(type.equals("WEAPONS VIOLATION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("WEAPONS_VIOLATION"), 1);
				}
				if(type.equals("DECEPTIVE PRACTICE")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("DECEPTIVE_PRACTICE"), 1);
				}
				if(type.equals("CRIM SEXUAL ASSAULT")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("CRIM_SEXUAL_ASSAULT"), 1);
				}
				if(type.equals("MOTOR VEHICLE THEFT")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("MOTOR_VEHICLE_THEFT"), 1);
				}
				if(type.equals("LIQUOR LAW VIOLATION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("LIQUOR_LAW_VIOLATION"), 1);
				}
				if(type.equals("PUBLIC PEACE VIOLATION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("PUBLIC_PEACE_VIOLATION"), 1);
				}
				if(type.equals("OTHER NARCOTIC VIOLATION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("OTHER_NARCOTIC_VIOLATION"), 1);
				}
				if(type.equals("OFFENSE INVOLVING CHILDREN")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("OFFENSE_INVOLVING_CHILDREN"), 1);
				}
				if(type.equals("INTERFERENCE WITH PUBLIC OFFICER")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("INTERFERENCE_WITH_PUBLIC_OFFICER"), 1);
				}
				if(type.equals("CONCEALED CARRY LICENSE VIOLATION")){
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("CONCEALED_CARRY_LICENSE_VIOLATION"), 1);
				}

				table.increment(increment);

			} catch (IOException e) {
				// TODO Auto-generated catch bloc
				e.printStackTrace();
			}finally {
				if(table != null)
					try {
						table.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp = "hadoop-w-1.c.mpcs53013-2015.internal";

		String zookeeperHost = zkIp +":2181";
		
		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp);
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "acidreflux-crime-events", "/acidreflux-crime-events","test_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkRoot = "/acidreflux-crime-events";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("raw-crime-events", kafkaSpout, 1);
		builder.setBolt("filter-reports", new FilterReportsBolt(), 1).shuffleGrouping("raw-crime-events");
		builder.setBolt("update-table", new UpdateCrimesBolt(), 1).fieldsGrouping("filter-reports", new Fields("ward"));

		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("crime-topology", conf, builder.createTopology());
		}
	}
}
