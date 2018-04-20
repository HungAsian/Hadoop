/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text guild = new Text();
        private Text name = new Text();
        private Text friend = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            if (itr.countTokens() >= 2) {
                name.set(itr.nextToken());
                guild.set(itr.nextToken());

                /*
                if(guild.toString().compareTo("N/A") != 0){
                    context.write(guild, name);
                }
                 */
                while (itr.hasMoreTokens()) {
                    friend.set(itr.nextToken());
                    Text FriendText = new Text();
                    Text NameText = new Text();
                    NameText.set("4" + name.toString());
                    FriendText.set("2" + friend.toString());
                    context.write(name, FriendText);
                    context.write(friend, NameText);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        //Check if list contains anything that starts with a 2, or if it has N/A if it contains any 4s
        private boolean Has2s = false;
        private boolean Has4s = false;
        private boolean HasNAs = false;

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> check = new ArrayList();
            for (Text val : values) {
                check.add(val.toString());
            }
            List<String> ToRemove = new ArrayList();
            //Loop through all values
            for (String str : check) {
                if (str.length() > 0) {
                    //If value starts with 4...
                    if (str.charAt(0) == '4') {
                        //store remaining friend name
                        String friend = str.substring(1);
                        //Loop through all lvalues, looking for start with 2 and remaining = friend
                        for (String secondStr : check) {
                            if (secondStr.length() > 0) {
                                if (secondStr.charAt(0) == '2' && secondStr.substring(1).compareTo(friend) == 0) {
                                    ToRemove.add(secondStr);
                                }
                            }
                        }
                    }
                }
            }

            check.removeAll(ToRemove);

            for (String str : check) {
                if (str.length() > 0) {
                    if (str.charAt(0) == '4') {
                        Has4s = true;
                    }
                    if (str.charAt(0) == '2') {
                        Has2s = true;
                    }
                }
            }

            if (check.contains("2N/A")) {
                HasNAs = true;
            }

            result.clear();

            if (HasNAs) {
                if (Has4s) {
                    result.set("Has an asymmetric friends list. (N/As)");
                } else {
                    result.set("Has a symmetric friends list. (N/As)");
                }
            } else if (!HasNAs) {
                if (Has2s) {
                    result.set("Has an asymmetric friends list.");
                } else {
                    result.set("Has a symmetric friends list.");
                }
            }

            result.set(result.toString() + "||" + String.valueOf(Has2s) + " " + String.valueOf(Has4s) + " " + String.valueOf(HasNAs));

            int cnt = 0;
            for (String val : check) {
                if (cnt == 0) {
                    result.set(val);
                } else {
                    result.set(result.toString() + ", " + val);
                }
                cnt++;
            }

            result.set(result.toString() + "||" + String.valueOf(Has2s) + " " + String.valueOf(Has4s) + " " + String.valueOf(HasNAs));
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        if (args[0].compareTo("T1") == 0) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(WordCount.TokenizerMapper.class);
            job.setCombinerClass(WordCount.IntSumReducer.class);
            job.setReducerClass(WordCount.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else if (args[0].compareTo("T2") == 0) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount2.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
