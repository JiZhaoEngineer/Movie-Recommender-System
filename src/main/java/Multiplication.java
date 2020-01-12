import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {

    public static class co_ocMatMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input--> movieB\tmovieA=relative_relation
            //output--> movieB\tmovieA=relative_relation
            String[] movie_relations = value.toString().trim().split("\t");
            context.write(new Text(movie_relations[0]), new Text(movie_relations[1]));
        }
    }

    public static class ratingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input--> userID,movieID,rating
            //output--> movieID\tuserID:rating
            String[] user_movie_rating = value.toString().trim().split(",");
            String movieId = user_movie_rating[1];
            String outputValue = user_movie_rating[0] + ":" + user_movie_rating[2];
            context.write(new Text(movieId), new Text(outputValue));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //input--> movieID\t<movieA1=relation1,movieA1=relation2,...user1:ratingA,user2:ratingA>
            //output--> movieA:user1\tsubMultiplication
            Map<String, Double> co_ocMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movieA_relation = value.toString().trim().split("=");
                    String movieA = movieA_relation[0];
                    double relation = Double.parseDouble(movieA_relation[1]);
                    co_ocMap.put(movieA, relation);
                }
                else {
                    String[] user_rating = value.toString().trim().split(":");
                    String userId = user_rating[0];
                    double rating = Double.parseDouble(user_rating[1]);
                    ratingMap.put(userId, rating);
                }
            }

            for (Map.Entry<String, Double> co_ocEntry : co_ocMap.entrySet()) {
                String movieA = co_ocEntry.getKey();
                double relation = co_ocEntry.getValue();
                for (Map.Entry<String, Double> ratingEntry : ratingMap.entrySet()) {
                    String userId = ratingEntry.getKey();
                    double rating = ratingEntry.getValue();

                    String outputKey = movieA + ":" + userId;
                    double subMultiplication = relation * rating;
                    
                    context.write(new Text(outputKey), new DoubleWritable(subMultiplication));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, co_ocMatMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, ratingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(co_ocMatMapper.class);
        job.setMapperClass(ratingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, co_ocMatMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ratingMapper.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
