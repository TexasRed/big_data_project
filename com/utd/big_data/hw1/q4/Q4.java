package com.utd.big_data.hw1.q4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Q4 class finds top 20 users whose friends have the greatest average age, 
 * it output the users' id with their address and the calculated average age.
 * 
 * Q4 Using reduce-side join and job chaining:
 * Step 1: Calculate the average age of the direct friends of each user.
 * Step 2: Sort the users by the calculated average age from step 1 in descending order.
 * Step 3. Output the top 20 users from step 2 with their address and the calculated average age.
 * 
 * @author KAI
 * @version 1.0
 * 
 */
public class Q4 {
	// The path of the tmp file that stores the output of job1
	private static final String TEMP_OUTPUT_PATH1 = "/kxh132430_intermediate_output1"; 
	// The path of the tmp file that stores the output of job2
	private static final String TEMP_OUTPUT_PATH2 = "/kxh132430_intermediate_output2";
	private static int counter = 0; // a counter to limit how many users from step2 are chosen
	private static final int TOP_N = 20; // the maximum users chosen from step2
	
	public static class Map1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable m_id = new IntWritable(); // the id of a given user
		private IntWritable m_age = new IntWritable(); // the age for a given user
		private static Calendar now = Calendar.getInstance(); // the current time
		// a in-memory HashMap that stores the age information for each user
		HashMap<Integer, Integer> userAgeMap = new HashMap<Integer, Integer>(); 
		
		// Load the user data, calculating the age for each user and store it in a map
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String myuserdataPath = conf.get("userdata");
			Path part=new Path("hdfs://cshadoop1" + myuserdataPath); // Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null){
					String[] arr = line.split(",");
					if(arr.length == 10){
						int userId = Integer.parseInt(arr[0].trim());
						int age = getAge(arr[9]);
						userAgeMap.put(userId, age); // Add the user age into a HashMap
					}
					line = br.readLine();
				}
			}
		}
		
		// Read the input file, emit the pair (user X, age of one of user X's friend)
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] userAndFriends = line.split("\t");
			if (userAndFriends.length == 2) {
				String user = userAndFriends[0];
				String[] friends = userAndFriends[1].split(",");
				m_id.set(Integer.parseInt(user));
				for (String friend : friends) {
					int age = userAgeMap.get(Integer.parseInt(friend));
					m_age.set(age);
					context.write(m_id, m_age); // write the user id and his age into context
				}
			}
		}
		
		/**
		 * Given a string that represents a birth date, return the age from the birth date till now
		 * @param birthdate a string representation of a birth date in the format of MM/dd/yyyy
		 * @return the age from the birth date till now
		 */
		public static int getAge(String birthdate) {
			Calendar dob = Calendar.getInstance();
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
			try {
				dob.setTime(format.parse(birthdate));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			if (dob.after(now)) {
				throw new IllegalArgumentException("Can't be born in the future");
			}
			int year1 = now.get(Calendar.YEAR);
			int year2 = dob.get(Calendar.YEAR);
			int age = year1 - year2;
			int month1 = now.get(Calendar.MONTH);
			int month2 = dob.get(Calendar.MONTH);
			if (month2 > month1) {
				age--;
			} else if (month1 == month2) {
				int day1 = now.get(Calendar.DAY_OF_MONTH);
				int day2 = dob.get(Calendar.DAY_OF_MONTH);
				if (day2 > day1) {
					age--;
				}
			}
			return age;
		}
	}
	// Calculate the average age of the friends for each user X
	public static class Reduce1 extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
		private IntWritable m_id = new IntWritable(); // the user id of a given user
		private DoubleWritable m_avg_age = new DoubleWritable(); // the average age of the friends of a given user

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int ageSum = 0; int count = 0;
			for (IntWritable value : values) {
				ageSum += value.get();
				count++;
			}
			int id = key.get();
			double avgAge = (double)(ageSum) / count;
			m_id.set(id);
			m_avg_age.set(avgAge);
			context.write(m_id, m_avg_age); // write the user id and the calculated average age to context
		}
	}
	
	// Create a composite key consisting of user id + user age, the value is set to NullWritable
	public static class Map2 extends Mapper<LongWritable, Text, UserAgeWritable, NullWritable> {
		private UserAgeWritable m_user_age_key = new UserAgeWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] userAvgAge = line.split("\t");
			if (userAvgAge.length == 2) {
				m_user_age_key.setUserId(Integer.parseInt(userAvgAge[0])); 
				m_user_age_key.setAvgAge(Double.parseDouble(userAvgAge[1]));
				context.write(m_user_age_key, NullWritable.get());
			}
		}
	}
	// The composite key is sorted by the average_age in descending order when it comes to reducer
	// If two users has friends of same average age, then the users were sorted by user_id in
	// ascending order.
	public static class Reduce2 extends Reducer<UserAgeWritable, NullWritable, IntWritable, DoubleWritable> {
		private IntWritable m_id = new IntWritable();
		private DoubleWritable m_avg_age = new DoubleWritable();

		public void reduce(UserAgeWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			// Getting the user id and average age from the composite key
//			String[] keyFields = key.toString().split(",");
//			String id = keyFields[0];
//			double avgAge = Double.parseDouble(keyFields[1]);
			int id = key.getUserId();
			double avgAge = key.getAvgAge();
			m_id.set(id);
			m_avg_age.set(avgAge);
			context.write(m_id, m_avg_age);
		}
	}	

	// Read user address from the user data file
	public static class Map3A extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable m_id = new IntWritable(); // The user id of a given user
		private Text m_address = new Text();// The user address tagged by "adr, e.g. "adr,187 Charla Lane,Plano,Texas"
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] userAttributes = line.split(",");
			if(userAttributes.length == 10){
				int id = Integer.parseInt(userAttributes[0].trim());
				String[] addressParts = {"adr", userAttributes[3], userAttributes[4], userAttributes[5]};
				String address = StringUtils.join(addressParts, ",");
				m_id.set(id);
				m_address.set(address);
				context.write(m_id, m_address);
			}
		}
	}
	// Read the friends' average age of a user from the tmp output file of job1
	// Write <user_id, avg_age> pair sorted by average age
	public static class Map3B extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable m_id = new IntWritable(); // The user id of a given user
		private Text m_avg_age = new Text(); // The average age tagged by "age", e.g. "age,85.0"
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (counter++ < TOP_N) {
				String line = value.toString();
				String[] userAvgAge = line.split("\t");
				if(userAvgAge.length == 2){
					int userId = Integer.parseInt(userAvgAge[0]);
					String avgAge = "age," + userAvgAge[1];
					m_id.set(userId);
					m_avg_age.set(avgAge);
					context.write(m_id, m_avg_age);
				}
			}
		}
	}
	// Perform a reducer-side join. Separate age and address information by tags
	// Do an in-memory join of the age with the address
	public static class Reduce3 extends Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable m_id = new IntWritable(); // the user id of a given user
		private Text m_info = new Text(); // the user info of the user including address and avg age
		private PriorityQueue<UserAgeWritable> queue = new PriorityQueue<>(); // a priority queue that sort the result by avg age
		// reducer-side join
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] userInfos = new String[2]; // A string array to help perform join
			for (Text value : values) {
				String val = value.toString();
				String tag = val.substring(0, 3); // Getting the tag
				String content = val.substring(4); // Stripping the tag
				switch (tag) {
				case "adr" :
					userInfos[0] = content;
					break;
				case "age":
					userInfos[1] = content;
					break;
				default : break;
				}
			}
			if (userInfos[1] != null) {
				int id = key.get();
				String address = userInfos[0];
				double avgAge = Double.parseDouble(userInfos[1]);
				queue.offer(new UserAgeWritable(id, avgAge, address));
			}
		}
		// Sort the joined result in their original order and output it
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (!queue.isEmpty()) {
				UserAgeWritable result = queue.poll();
				m_id.set(result.getUserId());
				m_info.set(result.getAddress() + "," + result.getAvgAge());
				context.write(m_id, m_info);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q4 <userdata>  <input> <output>");
			System.exit(2);
		}

		Path userDataPath = new Path(otherArgs[0]); // user data
		conf.set("userdata", otherArgs[0]);
		Path inputPath = new Path(otherArgs[1]); // input
		Path outputPath = new Path(otherArgs[2]);
		Path userAvgAgeFilePath = new Path(TEMP_OUTPUT_PATH1);
		Path sortedUserAvgAgeFilePath = new Path(TEMP_OUTPUT_PATH2);

		/*
		 * Job 1: Calculate the average age of the direct friends of each user.
		 */
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "AverageAge_Calculator");
		job1.setJarByClass(Q4.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, userAvgAgeFilePath);

		job1.waitForCompletion(true);
		
		/*
		 * Job 2: Sort the users by the calculated average age from step 1 in descending order.
		 */
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "AverageAge_Sorter");
		job2.setJarByClass(Q4.class);
		job2.setMapOutputKeyClass(UserAgeWritable.class);
		job2.setMapOutputValueClass(NullWritable.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		job2.setPartitionerClass(NaturalKeyPartitioner.class);
        job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job2.setSortComparatorClass(CompositeKeyComparator.class);

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, userAvgAgeFilePath);
		FileOutputFormat.setOutputPath(job2, sortedUserAvgAgeFilePath);

		job2.waitForCompletion(true);
		
		/*
		 * Job 3: Perform a reducer-side join, and output the top 20 users from step 2 
		 * with their address and the calculated average age.
		 */
		@SuppressWarnings("deprecation")
		Job job3 = new Job(conf, "UserInfo_Joiner");
		job3.setJarByClass(Q4.class);
		//output format for mapper
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(Text.class);
		//output format for reducer
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(Text.class);

		//use MultipleOutputs and specify different Record class and Input formats
		MultipleInputs.addInputPath(job3, userDataPath, TextInputFormat.class, Map3A.class);
		MultipleInputs.addInputPath(job3, sortedUserAvgAgeFilePath, TextInputFormat.class, Map3B.class);
		
		job3.setReducerClass(Reduce3.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job3, outputPath);

		job3.waitForCompletion(true);

	}
}
