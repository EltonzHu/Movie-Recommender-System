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


public class CoOccurrenceMatrixGenerator {
    public static class MatrixGeneratorMampper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //Input: userId\t movie1:rating1,move2:rating2

            String[] user_movieRating = value.toString().trim().split("\t");

            if (user_movieRating.length != 2) {
                return;
            }

            String[] movieRatings = user_movieRating[1].split(",");
            for (int i = 0; i < movieRatings.length; ++i) {
                String movie1 = movieRatings[i].split(":")[0];
                /*
                 * Each movie form a pair with all movies which this user rated
                 * ex. [1, 2, 3, 4] => 1,1 1,2 1,3 1,4 2,1 2,2 2,3 2,4...
                 *
                 * - Elton Hu (Oct.6, 2018)
                 */
                for (int j = 0; j < movieRatings.length; ++j) {
                    String movie2 = movieRatings[j].split(":")[0];
                    // Output: movie1:movie2 1
                    context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
                }
            }
        }
    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            // Output: movie1:movie2\t relationSum
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        job.setMapperClass(MatrixGeneratorMampper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
