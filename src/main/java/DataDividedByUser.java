import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataDividedByUser {

    public static class DataDividedByUserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input--> userID,movieID,rating
            //output--> userID\tmovieID:rating
            String[] user_movie_rating = value.toString().trim().split(",");
            String outputKey = user_movie_rating[0];
            String outputValue = user_movie_rating[1] + ":" + user_movie_rating[2];
            context.write(new IntWritable(Integer.parseInt(outputKey)), new Text(outputValue));
        }
    }

    public static class DataDiviedByUserReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //input--> userID\tmovieID:rating
            //output--> userID\tmovie1:rating1,movie2:rating2...
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()) {
                sb.append(values.iterator().next() + ",");
            }
            context.write(key, new Text(sb.deleteCharAt(sb.length() - 1).toString()));
        }
    }

    public static void main (String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDividedByUser.class);

        job.setMapperClass(DataDividedByUserMapper.class);
        job.setReducerClass(DataDiviedByUserReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
