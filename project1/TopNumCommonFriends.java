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

/**
 * Author: baojianfeng
 * Date: 2018-09-30
 * Usage: Find friend pairs whose number of common friends (number of mutual friend)
 *        is within the top-10 in all the pairs. Please output them in decreasing order.
 */
public class TopNumCommonFriends {


    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapOutKey = new LongWritable();
        private Text mapOutValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            // set friends pair as map output value
            mapOutValue.set(data[0]);
            String[] friends = data[1].split(",");
            // set the quantity of common friends as map output key
            mapOutKey.set((long) friends.length);
            // use the quantity as key, and sort it in descending order
            context.write(mapOutKey, mapOutValue);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int count = 0;
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (count < 10) {
                    // reduce output top 10
                    context.write(value, key);
                    count++;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];

        // remove output directory if it exists
        FileUtil.removeOutputDir(conf, otherArgs[1]);

        conf = new Configuration();
        Job job = new Job(conf, "TOP10List");

        job.setJarByClass(TopNumCommonFriends.class);
        job.setMapperClass(TopNumCommonFriends.Map.class);
        job.setReducerClass(TopNumCommonFriends.Reduce.class);

        // sort map key in descending order
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            FileUtil.printFileContent(conf, otherArgs[1] + "/part-r-00000");
            System.out.println("success");
        } else {
            System.out.println("fail");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
