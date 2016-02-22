package com.utd.big_data.hw1.q1;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
/**
 * Q1 class recommends at most N = 10 users who are not already friends with U, 
 * but have the largest number of mutual friends in common with U
 * 
 * @author KAI
 * @version 1.0
 * 
 */

public class Q1 {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    	// For each friend of a user X, tag the friend as the direct friend of user X
    	// For any two friends of user X, tag the two friends as 2-level friend of each other
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] userAndFriends = line.split("\t");
            if (userAndFriends.length == 2) {
                String user = userAndFriends[0];
                IntWritable user_id = new IntWritable(Integer.parseInt(user));
                String[] friends = userAndFriends[1].split(",");
                String friend1;
                IntWritable friend1Key = new IntWritable();
                Text friend1Value = new Text();
                String friend2;
                IntWritable friend2Key = new IntWritable();
                Text friend2Value = new Text();
                for (int i = 0; i < friends.length; i++) {
                    friend1 = friends[i];
                    friend1Value.set("1," + friend1);
                    context.write(user_id, friend1Value);   // direct friend
                    friend1Key.set(Integer.parseInt(friend1));
                    friend1Value.set("2," + friend1);
                    for (int j = i + 1; j < friends.length; j++) {
                        friend2 = friends[j];
                        friend2Key.set(Integer.parseInt(friend2));
                        friend2Value.set("2," + friend2);
                        context.write(friend1Key, friend2Value); // 2-level friend
                        context.write(friend2Key, friend1Value); // 2-level friend
                    }
                }
            }
        }
    } 
 
    // 
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
    	private static int MAX_RECOMMENDATION_FRIENDS = 10; // the maximum friends to recommend for a given user
    	// Count the number of common friends for a given user(value[1]) and the key
        // given that values[1] and key are 2-level friends
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value = new String[2];
            HashMap<String, Integer> commonFriendsCountMap = new HashMap<String, Integer>();
            for (Text val : values) {
                value = (val.toString()).split(",");
                if (value[0].equals("1")) { // if key and value[1] are direct friend, mark this pair as -1
                    commonFriendsCountMap.put(value[1], -1);
                } else if (value[0].equals("2")) {
                	// if key and value[1] are 2-level but not direct friend,
                	// we can recommended values[1] as a new friend to key
                    if (commonFriendsCountMap.containsKey(value[1])) {
                        if (commonFriendsCountMap.get(value[1]) != -1) {
                            commonFriendsCountMap.put(value[1], commonFriendsCountMap.get(value[1]) + 1);
                        }
                    } else {
                        commonFriendsCountMap.put(value[1], 1);
                    }
                }
            }
            
            // Convert the HashMap into a list, remove the 1-level friend pairs from the list
            List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
            for (Entry<String, Integer> entry : commonFriendsCountMap.entrySet()) {
                if (entry.getValue() != -1) {   // Remove the friend pair who are direct friends
                    list.add(entry);
                }
            }
            
            // Sort key-value pairs in the list by values (number of mutual friends).
            // If there are multiple users with the same number of mutual friends, 
            // sort them in a numerically ascending order of their user IDs.
            Collections.sort(list, new Comparator<Entry<String, Integer>>() {
                public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
                    int diff = e2.getValue().compareTo(e1.getValue());
                    if (diff == 0)
                    	return Integer.parseInt(e1.getKey()) - (Integer.parseInt(e2.getKey()));
                    else
                    	return diff;
                }
            });
            
            // Recommend at most 10 people that a user might know, ordered by decreasing number of mutual friends.
            // Even if a user has fewer than 10 second-degree friends, output all of them in decreasing order of
            // the number of mutual friends.
            if (MAX_RECOMMENDATION_FRIENDS < 1) {
                // Output all recommended friends for each user
                context.write(key, new Text(StringUtils.join(list, ",")));
            } else {
                // Output at most MAX_RECOMMENDATION_FRIENDS keys with the highest values (number of common friends)
                List<String> top = new ArrayList<String>();
                for (int i = 0; i < Math.min(MAX_RECOMMENDATION_FRIENDS, list.size()); i++) {
                    top.add(list.get(i).getKey());
                }
                context.write(key, new Text(StringUtils.join(top, ",")));
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    	if (otherArgs.length != 2) {
			System.err.println("Usage: Q1 <in> <out>");
			System.exit(2);
		}
		
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "FriendRecommender");
        job.setJarByClass(Q1.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 
        job.waitForCompletion(true);
    }
}
