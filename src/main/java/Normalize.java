import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input--> movieA:movieB\tsum
            //output--> movieA\tmovieB=sum
            //sum is absolute relation
            String[] movies_relation = value.toString().trim().split("\t");
            if (movies_relation.length != 2) {
                return;
            }
            String[] movies = movies_relation[0].split(":");
            String outputKey = movies[0];//movieA
            String outputValue = movies[1] + "=" + movies_relation[1];//movieB=relation
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //input--> movieA\tmovieB=absolute_relation
            //output--> movieB\tmovieA=relative_relation
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {
                String[] movieB_relation = value.toString().split("=");
                String movieB = movieB_relation[0];
                int relation = Integer.parseInt(movieB_relation[1]);
                map.put(movieB, relation);
                sum += relation;
            }

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key + "=" + (double)entry.getValue()/sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Normalize.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
