import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Author: baojianfeng
 * Date: 2018-09-30
 * Usage: Given any two people(they are friends),
 *        use in-memory join to output the list of
 *        the names and the states of the their mutual friends
 */
public class FindNameAndState {


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private java.util.Map<String, String> userMap = new HashMap<>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private String targetUserA, targetUserB;
        private boolean found = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            conf.addResource("etc/hadoop/core-site.xml");
            conf.addResource("etc/hadoop/hdfs-site.xml");
            // path where userdata.txt stored in hdfs
            String userDataPath = conf.get("userdata");
            targetUserA = conf.get("target_user_a");
            targetUserB = conf.get("target_user_b");
            Path path = new Path(userDataPath);
            // TODO write into summary, https://blog.csdn.net/qq_20545159/article/details/49472993
            FileSystem fs = path.getFileSystem(conf);
            if (!fs.exists(path)) {
                System.out.println("File: " + userDataPath + " doesn't exist");
                return;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            while (line != null) {
                String[] data = line.split(",");
                if (data.length == 10) {
                    // store user info into a hashmap
                    userMap.put(data[0], data[1] + ": " + data[5]); // data[0]: userid, data[1]: user first name, data[5]: state
                }
                line = br.readLine();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            // data[0] store the friends pair, such as: (0, 1)
            String[] users = data[0].split(",");
            if (!users[0].equals(targetUserA) || !users[1].equals(targetUserB))
                return;

            found = true;
            outputKey.set(data[0]);
            String[] friends = data[1].split(",");
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (String fri : friends) {
                if (userMap.containsKey(fri)) {
                    sb.append(userMap.get(fri));
                    sb.append(", ");
                }
            }
            int len = sb.length();
            sb.delete(len - 2, len); // delete extra ", " at last
            sb.append("]");
            outputValue.set(sb.toString()); // [evangeline: Ohio, Charlotte: California]
            context.write(outputKey, outputValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!found) {
                outputKey.set("No such friends pair exists");
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String mutualFriendsPath = otherArgs[0];
        String nameStatesOutPath = otherArgs[1];
        String userDataPath = otherArgs[2];
        String targetUserA = otherArgs[3];
        String targetUserB = otherArgs[4];

        // remove output directory if exists
        FileUtil.removeOutputDir(conf, otherArgs[1]);

        conf = new Configuration();
        conf.set("userdata", userDataPath); // set userdata file path to configuration object
        conf.set("target_user_a", targetUserA);
        conf.set("target_user_b", targetUserB);

        Job job = new Job(conf, "FindNameAndState");
        job.setJarByClass(FindNameAndState.class);
        job.setMapperClass(FindNameAndState.Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // no reduce task
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(mutualFriendsPath)); // the file of all mutual friend list
        FileOutputFormat.setOutputPath(job, new Path(nameStatesOutPath));

        if (job.waitForCompletion(true)) {
            FileUtil.printFileContent(conf, otherArgs[1] + "/part-m-00000");
            System.out.println("success");
        } else {
            System.out.println("fail");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
