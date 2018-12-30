import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: baojianfeng
 * Date: 2018-09-30
 * Usage: Using reduce-side join and job chaining:
 *        Step 1: Calculate the minimum age of the direct friends of each user.
 *        Step 2: Sort the users by the calculated minimum age from step 1 in descending order.
 *        Step 3. Output the top 10 users from step 2 with their address and the calculated minimum age.
 */
public class TopMinAgeOfFriends {

    // job1
    public static class UserDataAgeMap extends Mapper<LongWritable, Text, KeyPair, NullWritable> {
        private KeyPair mapOutKey = new KeyPair();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");

            int age = TimeUtil.calAge(data[9]);
            // set composite key
            mapOutKey.setFirst(data[0]);
            mapOutKey.setSecond("A:" + age);

            context.write(mapOutKey, NullWritable.get());
        }
    }

    public static class ListFriendsMap extends Mapper<LongWritable, Text, KeyPair, NullWritable> {
        private KeyPair mapOutKey = new KeyPair();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            // suppose we have 0 1,2,3
            // we will set the outKey to (1, B:0), (2, B:0), (3, B:0)
            mapOutKey.setSecond("B:" + data[0]);

            if (data.length != 2)
                return;

            String[] friends = data[1].split(",");
            for (String fri : friends) {
                mapOutKey.setFirst(fri);
                context.write(mapOutKey, NullWritable.get());
            }
        }
    }

    public static class MinAgeOfFriendsReduce extends Reducer<KeyPair, NullWritable, Text, IntWritable> {
        private Map<String, Integer> minAgeMap = new HashMap<>();
        private int age;

        @Override
        protected void reduce(KeyPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // use the above mapper and KeyPair, make sure the first key in a reducer is the age related one
            if (key.getSecond().toString().startsWith("A")) {
                String[] secondKey = key.getSecond().toString().split(":");
                age = Integer.parseInt(secondKey[1]);
            } else {
                String[] strs = key.getSecond().toString().split(":");
                if (minAgeMap.containsKey(strs[1])) {
                    int minAge = minAgeMap.get(strs[1]);
                    if (age < minAge)
                        minAgeMap.put(strs[1], age);
                } else {
                    minAgeMap.put(strs[1], age);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : minAgeMap.entrySet()) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    // job2
    public static class SwapUserAgeMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapOutKey = new LongWritable();
        private Text mapOutValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            // set age as key
            mapOutKey.set(Long.parseLong(data[1]));
            mapOutValue.set(data[0]);
            context.write(mapOutKey, mapOutValue);
        }

    }

    public static class MinAgeDesReduce extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int count = 0;
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                if (count < 10) {
                    // find top 10 minimum age
                    context.write(text, key);
                    count++;
                }
            }
        }
    }

    // job3
    public static class Top10UserMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutKey = new Text();
        private Text mapOutValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            mapOutKey.set(data[0]);
            mapOutValue.set(data[1]);
            context.write(mapOutKey, mapOutValue);
        }
    }

    public static class AddressMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutKey = new Text();
        private Text mapOutValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            mapOutKey.set(data[0]);
            mapOutValue.set(data[1] + "," + data[3] + "," + data[4] + "," + data[5] + "address");
            context.write(mapOutKey, mapOutValue);
        }
    }

    public static class FinalOutputReduce extends Reducer<Text, Text, Text, NullWritable> {
        private Text reduceOutKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String name = "";
            String addr = "";
            String age = "";
            for (Text text : values) {
                if (text.toString().endsWith("address")) {
                    String val = text.toString();
                    int len = val.length();
                    String addrName = val.substring(0, len - 7);
                    String[] strs = addrName.split(",", 2); // split name and address
                    name = strs[0];
                    addr = strs[1]; // delete address in the end
                } else {
                    age = text.toString();
                }
            }

            if (!name.isEmpty() && !addr.isEmpty() && !age.isEmpty()) {
                sb.append(name);
                sb.append(",");
                sb.append(addr);
                sb.append(",");
                sb.append(age);
                reduceOutKey.set(sb.toString());
                context.write(reduceOutKey, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        String socFilePath = otherArgs[0];
        String userFilePath = otherArgs[1];
        String finalOutPath = otherArgs[2];
        String tmpPathJob1 = otherArgs[3];
        String tmpPathJob2 = otherArgs[4];

        FileUtil.removeOutputDir(conf, finalOutPath);
        FileUtil.removeOutputDir(conf, tmpPathJob1);
        FileUtil.removeOutputDir(conf, tmpPathJob2);

        // job1 calculate minimum age of direct friend
        {
            conf = new Configuration();
            Job job1 = Job.getInstance(conf, "CalMinAge");

            job1.setJarByClass(TopMinAgeOfFriends.class);
            job1.setMapperClass(UserDataAgeMap.class);
            job1.setMapperClass(ListFriendsMap.class);
            job1.setReducerClass(MinAgeOfFriendsReduce.class);

            job1.setMapOutputKeyClass(KeyPair.class);
            job1.setMapOutputValueClass(NullWritable.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            MultipleInputs.addInputPath(job1, new Path(userFilePath), TextInputFormat.class, UserDataAgeMap.class);
            MultipleInputs.addInputPath(job1, new Path(socFilePath), TextInputFormat.class, ListFriendsMap.class);
            FileOutputFormat.setOutputPath(job1, new Path(tmpPathJob1));

            if (job1.waitForCompletion(true)) {
//                FileUtil.printFileContent(conf, tmpPathJob1 + "/part-r-00000");
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            if (!job1.waitForCompletion(true))
                System.exit(1);
        }

        // job2 find top 10 minimum age
        {
            conf = new Configuration();
            Job job2 = Job.getInstance(conf, "TOP10MinAge");

            job2.setJarByClass(TopMinAgeOfFriends.class);
            job2.setMapperClass(SwapUserAgeMap.class);
            job2.setReducerClass(MinAgeDesReduce.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);

            // sort map key in descending order
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            FileInputFormat.addInputPath(job2, new Path(tmpPathJob1));
            FileOutputFormat.setOutputPath(job2, new Path(tmpPathJob2));

            if (job2.waitForCompletion(true)) {
//                FileUtil.printFileContent(conf, tmpPathJob2 + "/part-r-00000");
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            if (!job2.waitForCompletion(true))
                System.exit(1);
        }

        // job3 find the final result, including name, address, minimum age of friend
        {
            conf = new Configuration();
            Job job3 = Job.getInstance(conf, "TopUserAddressMinAge");

            job3.setJarByClass(TopMinAgeOfFriends.class);
            job3.setMapperClass(Top10UserMap.class);
            job3.setMapperClass(AddressMap.class);
            job3.setReducerClass(FinalOutputReduce.class);

            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(NullWritable.class);

            MultipleInputs.addInputPath(job3, new Path(tmpPathJob2), TextInputFormat.class, Top10UserMap.class);
            MultipleInputs.addInputPath(job3, new Path(userFilePath), TextInputFormat.class, AddressMap.class);
            FileOutputFormat.setOutputPath(job3, new Path(finalOutPath));

            if (job3.waitForCompletion(true)) {
                FileUtil.printFileContent(conf, finalOutPath + "/part-r-00000");
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }
    }

    public static class KeyPair implements WritableComparable<KeyPair> {
        private Text userId = new Text();
        private Text data = new Text();

        public KeyPair() {}

        public KeyPair(String userIdStr, String dataStr) {
            this.userId = new Text(userIdStr);
            this.data = new Text(dataStr);
        }

        public void setFirst(String userIdStr) {
            this.userId.set(userIdStr);
        }

        public void setSecond(String dataStr) {
            this.data.set(dataStr);
        }

        public Text getFirst() {
            return userId;
        }

        public Text getSecond() {
            return data;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            userId.write(dataOutput);
            data.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            userId.readFields(dataInput);
            data.readFields(dataInput);
        }

        @Override
        public int compareTo(KeyPair o) {
            int comp = userId.compareTo(o.getFirst());
            if (comp != 0)
                return comp;
            return data.compareTo(o.getSecond());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof KeyPair) {
                KeyPair kp = (KeyPair) obj;
                return userId.equals(kp.getFirst()) && data.equals(kp.getSecond());
            }
            return false;
        }

        @Override
        public int hashCode() {
            //  partition by userId
            return userId.hashCode();
        }
    }
}
