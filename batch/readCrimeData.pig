REGISTER /usr/local/elephant-bird/elephant-bird-core-4.10.jar;
REGISTER /usr/local/elephant-bird/elephant-bird-pig-4.10.jar;
REGISTER /usr/local/elephant-bird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER /usr/hdp/2.2.8.0-3150/hive/lib/libthrift-0.9.0.jar;
REGISTER /usr/hdp/2.2.8.0-3150/pig/lib/piggybank.jar;
REGISTER /mnt/scratch/acidreflux/uber-serialCrime-0.0.1-SNAPSHOT.jar

DEFINE WSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('edu.uchicago.mpcs53013.crimeSummary.CrimeSummary');
RAW_DATA = LOAD '/mnt/scratch/acidreflux/data/*' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
CRIME_SUMMARY = FOREACH RAW_DATA GENERATE FLATTEN(WSThriftBytesToTuple(value));
STORE CRIME_SUMMARY into '/mnt/scratch/acidreflux/pigfiles' Using PigStorage(',');
