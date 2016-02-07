package edu.mpcs53013.serialCrime;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import edu.uchicago.mpcs53013.crimeSummary.CrimeSummary;

public class SerializeCrimeSummary {
	static TProtocol protocol;

	
	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			final Configuration finalConf = new Configuration(conf);
			final FileSystem fs = FileSystem.get(conf);
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			CrimeSummaryProcessor processor = new CrimeSummaryProcessor() {
				Map<String, SequenceFile.Writer> nameMap = new HashMap<String, SequenceFile.Writer>();
				
				Writer getWriter(File file) throws IOException {
					String name = file.getName();
					if(!nameMap.containsKey(name)) {
						nameMap.put(name, 
								SequenceFile.createWriter(finalConf,
										SequenceFile.Writer.file(
												new Path("/mnt/scratch/acidreflux/data/" + name)),
										SequenceFile.Writer.keyClass(IntWritable.class),
										SequenceFile.Writer.valueClass(BytesWritable.class),
										SequenceFile.Writer.compression(CompressionType.NONE)));
					}
					return nameMap.get(name);
				}

				@Override
				void processCrimeSummary(CrimeSummary summary, File file) throws IOException {
					
					try {
						getWriter(file).append(new IntWritable(1), new BytesWritable(ser.serialize(summary)));

					} catch (TException e) {
						throw new IOException(e);
					}
				}
			};
			processor.processCrimeDirectory(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
