package com.utd.big_data.hw1.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Q2 class finds the mutual friends(id) for any two users. Given any two Users as input, 
 * the class will output the list of the user id of their mutual friends.
 * 
 * @author KAI
 * @version 1.0
 * 
 */
public class Q2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text m_id = new Text(); // the combined user id of two given users
		private Text m_friends = new Text(); // the user ids of the primary users' friends
		private Text m_query_uid = new Text(); // the combined userId of the two querying uids: userID1 and userID2
		private static boolean hasCommonFriends = false; // whether the two query user has mutual friends
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			m_query_uid.set(conf.get("queryUID"));
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Split each line by tab to separate the user and its friends list.
			String line = value.toString();
			String[] userAndFriends = line.split("\t");
			if (userAndFriends.length == 2) {
				String user = userAndFriends[0];
				String[] query_friends = m_query_uid.toString().split(",");
				if (user.equals(query_friends[0]) || user.equals(query_friends[1])) {
					hasCommonFriends = true;
					m_id.set(m_query_uid);
					m_friends.set(userAndFriends[1]);
					context.write(m_id, m_friends);
				}
			}
		}

		// If two users have no common friends, output two empty friend lists
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (!hasCommonFriends) {
				context.write(m_query_uid, new Text(""));
				context.write(m_query_uid, new Text(""));
			}
		}
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		//A list of the user id of the mutual friends of two given users
		private Text m_mutualFriends = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Store the friends lists of a given friends pair in an array of String
			String[] friendLists = new String[2];
			int idx = 0;
			for(Text value : values) {
				friendLists[idx++] = value.toString();
			}
			// Calculate the intersection of these lists and write result in the form (UserA, UserB, MutualFriends).
			m_mutualFriends.set(intersection(friendLists[0], friendLists[1]));
			context.write(key, m_mutualFriends);
		}

		// Calculates intersection of two friends lists
		private String intersection(String s1, String s2) {
			if (s1 == null) s1 = "";
			if (s2 == null) s2 = "";
			List<String> friendList1 = new ArrayList<>(Arrays.asList(s1.split(",")));
			List<String> friendList2 = new ArrayList<>(Arrays.asList(s2.split(",")));
			friendList1.retainAll(friendList2);
			String commonFriendList = StringUtils.join(friendList1, ",");
			return "\t" + commonFriendList;
		}
	}

	/**
	 * Given two user ids, return the combined user id
	 * @param userA the first user id
	 * @param userB the second user id
	 * @return a combined user id concatenated by comma and sorted by value
	 */
	private static String getCombinedID(String userA, String userB) {
		return (Integer.parseInt(userA)) < (Integer.parseInt(userB)) ? userA + "," + userB : userB + "," + userA;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: Q2 <in> <out> <userA> <userB> ");
			System.exit(2);
		}
		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);

		String userA = otherArgs[2];
		String userB = otherArgs[3];
		String queryUID = getCombinedID(userA, userB);
		conf.set("queryUID", queryUID);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriendFinder");
		job.setJarByClass(Q2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}
}
