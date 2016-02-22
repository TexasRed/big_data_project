package com.utd.big_data.hw1.q4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected NaturalKeyGroupingComparator() {
		super(UserAgeWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserAgeWritable k1 = (UserAgeWritable)w1;
		UserAgeWritable k2 = (UserAgeWritable)w2;
		Integer uid_1 = new Integer(k1.getUserId());
		Integer uid_2 = new Integer(k2.getUserId());
		return uid_1.compareTo(uid_2);
	}
}
