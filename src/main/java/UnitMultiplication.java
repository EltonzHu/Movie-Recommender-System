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
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;


public class UnitMultiplication {
    public static class CooccurrenceNormalizationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input: movieB\t movieA=normalizedRelationValue
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input: user, movie, rating
            String[] line = value.toString().split(",");
            context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
        }
    }

    public static class UnitMultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            /*
             * Input:
             * Key = movieB
             * Value = <movieA=normalizedRelation, movieC=normalizedRelation, userA:rating, userB:rating, ...>
             */

            Map<String, Double> movieRelationMap = new HashMap<String, Double>();
            Map<String, Double> userRatingMap = new HashMap<String, Double>();

            for (Text value: values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().split("=");
                    movieRelationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else {
                    String[] user_rating = value.toString().split(":");
                    userRatingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            for (Map.Entry<String, Double> entry : movieRelationMap.entrySet()) {
                String movie = entry.getKey();
                double relation = entry.getValue();

                for (Map.Entry<String, Double> unit : userRatingMap.entrySet()) {
                    String user = unit.getKey();
                    double rating = unit.getValue();

                    String outputKey = user + ":" + movie;
                    double outputValue = relation * rating;
                    context.write(new Text(outputKey), new DoubleWritable((outputValue)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        job.setJarByClass(UnitMultiplication.class);

        /*
         * Important!!!
         * Use a different approach for matrix multiplication compared to PageRank project UnitMultiplication;
         * However, keep in mind that those two different approaches sharing same outputs.
         * ChainMapper is smart enough knowing those two mappers are not chaining together by checking input parameters.
         * Those two mappers are in parallel.
         *
         * - Elton Hu (Oct.7, 2018)
         */

        ChainMapper.addMapper(job, CooccurrenceNormalizationMapper.class, Object.class, Text.class, Text.class,
                Text.class, config);
        ChainMapper.addMapper(job, RatingMapper.class, Object.class, Text.class, Text.class, Text.class, config);

        job.setMapperClass(CooccurrenceNormalizationMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(UnitMultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceNormalizationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
