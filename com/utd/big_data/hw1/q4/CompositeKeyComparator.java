package com.utd.big_data.hw1.q4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(UserAgeWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserAgeWritable k1 = (UserAgeWritable)w1;
		UserAgeWritable k2 = (UserAgeWritable)w2;
		Integer uid_1 = new Integer(k1.getUserId());
		Integer uid_2 = new Integer(k2.getUserId());
		
		Double avgAge_1 = k1.getAvgAge();
		Double avgAge_2 = k2.getAvgAge();
		int result = -1 * avgAge_1.compareTo(avgAge_2);
		if (result == 0) return uid_1.compareTo(uid_2);
		else return result;
	}
}
