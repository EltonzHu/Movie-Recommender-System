/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with Hadoop framework for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DataDividerByUser {
    public static class DataDividerMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Input data format: User, movie, rating
            String[] user_movie_rating = value.toString().trim().split(",");
            int userId = Integer.parseInt(user_movie_rating[0]);
            String movieId = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new IntWritable(userId), new Text(movieId + ":" + rating));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();

            while(values.iterator().hasNext()) {
                sb.append("," + values.iterator().next());
            }

            //Output: userId\t movie1:rating1,move2:rating2...
            context.write(key, new Text(sb.toString().replaceFirst(",", "")));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setJarByClass(DataDividerByUser.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Text format by default
        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
