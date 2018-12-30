import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Author: baojianfeng
 * Date: 2018-09-30
 * Usage: find common friends list of two people if they are friends
 */
public class CommonFriends {

    // first job Map
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutKey = new Text();
        private Text friendsList = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split by tab
            String[] lineData = value.toString().split("\\t");
            if (lineData.length != 2)
                return;
            friendsList.set(lineData[1]);
            String[] friendArr = lineData[1].split(",");
            long keyLongValue = Long.parseLong(lineData[0]);
            for (String friend : friendArr) {
                long friendLongValue = Long.parseLong(friend);
                // make sure friends pair's first userid is less than second userid
                if (keyLongValue < friendLongValue) {
                    mapOutKey.set(lineData[0] + "," + friend);
                }
                else if (keyLongValue > friendLongValue) {
                    mapOutKey.set(friend + "," + lineData[0]);
                }

                context.write(mapOutKey, friendsList);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        // first job Reduce
        private Text commonFriend = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friendSet = new HashSet<>();
            List<String> tempList = new ArrayList<>();

            for (Text text : values) {
                tempList.add(text.toString());
            }

            // final common friends list store in StringBuffer
            StringBuilder sb = new StringBuilder();
            if (tempList.size() == 2) {
                // store friend list 1 into hashset
                String friList1 = tempList.get(0);
                String[] friList1Arr = friList1.split(",");
                for (String friend : friList1Arr) {
                    friendSet.add(friend);
                }

                String friList2 = tempList.get(1);
                String[] friList2Arr = friList2.split(",");
                for (String friend : friList2Arr) {
                    // find common friends
                    if (friendSet.contains(friend)) {
                        sb.append(friend);
                        sb.append(",");
                        friendSet.remove(friend); // in case there are any duplicate friend
                    }
                }

                if (sb.length() > 0) {
                    // delete the last comma
                    sb.deleteCharAt(sb.length() - 1);
                    commonFriend.set(sb.toString());
                    context.write(key, commonFriend);
                }
            }
        }
    }

    // second job Map
    public static class ExtractMap extends Mapper<LongWritable, Text, Text, Text> {
        private java.util.Map<String, String> extractKeyMap = new HashMap<>();
        private Text mapOutKey = new Text();
        private Text result = new Text();

        public ExtractMap() {
            extractKeyMap.put("0", "1");
            extractKeyMap.put("20", "28193");
            extractKeyMap.put("1", "29826");
            extractKeyMap.put("6222", "19272");
            extractKeyMap.put("28041", "28056");
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            String[] keyVal = data[0].split(",");
            if (extractKeyMap.containsKey(keyVal[0])) {
                if (extractKeyMap.get(keyVal[0]).equals(keyVal[1])) {
                    mapOutKey.set(data[0]);
                    result.set(data[1]);
                    context.write(mapOutKey, result);
                    extractKeyMap.remove(keyVal[0], keyVal[1]); // remove the key-value
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<String, String> entry : extractKeyMap.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), new Text());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        String tempPath = otherArgs[2]; // temp path for job1's output

        // remove output directory
        FileUtil.removeOutputDir(conf, otherArgs[1]);
        FileUtil.removeOutputDir(conf, otherArgs[2]);

        // first job
        {
            conf = new Configuration();
            Job job1 = Job.getInstance(conf, "CommonFriends");

            job1.setJarByClass(CommonFriends.class);
            job1.setMapperClass(CommonFriends.Map.class);
            job1.setReducerClass(CommonFriends.Reduce.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(inputPath));
            FileOutputFormat.setOutputPath(job1, new Path(tempPath));

            if (job1.waitForCompletion(true)) {
//                printFileContent(conf, otherArgs[2] + "/part-r-00000");
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            if (!job1.waitForCompletion(true))
                System.exit(1);
        }

        // second job
        {
            conf = new Configuration();
            Job job2 = new Job(conf, "ExtractPairs");

            job2.setJarByClass(CommonFriends.class);
            job2.setMapperClass(CommonFriends.ExtractMap.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // don't have reduce task
            job2.setNumReduceTasks(0);

            // use the temp output path as the input path of job2
            FileInputFormat.addInputPath(job2, new Path(tempPath));
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            if (job2.waitForCompletion(true)) {
                FileUtil.printFileContent(conf, otherArgs[1] + "/part-m-00000");
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
