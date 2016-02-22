package com.utd.big_data.hw1.q4;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<UserAgeWritable, NullWritable> {
	@Override
	public int getPartition(UserAgeWritable key, NullWritable val, int numPartitions) {
		int hash = new Integer(key.getUserId()).hashCode();
		int partition = hash % numPartitions;
		return partition;
	}
}
