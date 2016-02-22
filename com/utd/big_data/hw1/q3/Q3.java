package com.utd.big_data.hw1.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
 * Q3 class uses in-memory join to find the mutual friends(id and first name) 
 * for any two users. Given any two Users as input, Q3 will output the list of
 * the names and the zipcode of their mutual friends.
 * 
 * @author KAI
 * @version 1.0
 * 
 */
public class Q3 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text m_id = new Text(); // the combined user id of two given users
		private Text m_friends_info = new Text(); // the user info of the primary users' friends
		private Text m_query_uid = new Text(); // the combined userId of the two querying uids: userID1 and userID2
		HashMap<String,String> m_userInfoMap; // a in-memory HashMap to store the user information (name + zipcode)
		private static boolean hasCommonFriends = false; // whether the two query user has mutual friends

		// Load the user data, store the name and zip code for each user and store them in a map
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			// Get query uid in the format: "userA_ID,userB_ID" (userA_ID < userB_ID)
			Configuration conf = context.getConfiguration();
			m_query_uid.set(conf.get("queryUID"));

			//read user data to memory on the mapper.
			m_userInfoMap = new HashMap<String,String>();
			String myuserdataPath = conf.get("userdata");
			Path part=new Path("hdfs://cshadoop1" + myuserdataPath); //Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null){
					String[] userAttributes = line.split(",");
					if(userAttributes.length == 10){
						String id = userAttributes[0].trim();
						String userInfo = userAttributes[1] + ":" + userAttributes[6]; // userInfo = name + zipcode
						m_userInfoMap.put(id, userInfo); // Add the user info into a HashMap
					}
					line = br.readLine();
				}
			}
		}
		// Perform the mapper-side join, the user data in the userInfoMap are shared by all mappers.
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Split each line by tab to separate the user and its friends list.
			String line = value.toString();
			String[] userAndFriends = line.split("\t");
			if (userAndFriends.length == 2) {
				String user = userAndFriends[0];
				String[] friends = userAndFriends[1].split(",");
				String[] query_friends = m_query_uid.toString().split(",");
				if (user.equals(query_friends[0]) || user.equals(query_friends[1])) {
					hasCommonFriends = true;
					m_id.set(m_query_uid);
					List<String> friendInfoList = new ArrayList<>();
					for (String friend : friends) {
						String friendInfo = m_userInfoMap.get(friend); // Get user info from map
						friendInfoList.add(friendInfo);
					}
					m_friends_info.set(StringUtils.join(friendInfoList, ","));
					context.write(m_id, m_friends_info);
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
	// Get the mututal friends for a given friend pair, output the user info for these users
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		//A list of the names and the zipcode of the mutual friends of two given users
		private Text m_mutual_friend_info = new Text(); 

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Store the friends lists of a given friends pair in an array of String
			String[] friendLists = new String[2];
			int idx = 0;
			for(Text value : values) {
				friendLists[idx++] = value.toString();
			}
			// Calculate the intersection of these lists and write result in the form (UserA, UserB, MutualFriends).
			m_mutual_friend_info.set(intersection(friendLists[0], friendLists[1]));
			context.write(key, m_mutual_friend_info);
		}

		// Calculates intersection of two friends lists
		private String intersection(String s1, String s2) {
			if (s1 == null) s1 = "";
			if (s2 == null) s2 = "";
			List<String> friendList1 = new ArrayList<>(Arrays.asList(s1.split(",")));
			List<String> friendList2 = new ArrayList<>(Arrays.asList(s2.split(",")));
			friendList1.retainAll(friendList2);
			return "\t" + friendList1.toString();
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: Q3 <userdata> <in> <out> <userA> <userB> ");
			System.exit(2);
		}

		conf.set("userdata", otherArgs[0]);
		Path inputPath = new Path(otherArgs[1]);
		Path outputPath = new Path(otherArgs[2]);

		String userA = otherArgs[3];
		String userB = otherArgs[4];
		String queryUID = getCombinedID(userA, userB);
		conf.set("queryUID", queryUID);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriend_UserInfo_Joiner");
		job.setJarByClass(Q3.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}
}
