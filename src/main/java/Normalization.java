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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalization {

    public static class NormalizationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Input: movieA:movieB\t relationSum
            String[] movie_relationSum = value.toString().trim().split("\t");
            if (movie_relationSum.length != 2) {
                return;
            }

            String[] movies = movie_relationSum[0].split(":");

            context.write(new Text(movies[0]), new Text(movies[1] + "=" + movie_relationSum[1]));
        }
    }

    public static class NormalizationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();

            for (Text value : values) {
                String[] movie_relation = value.toString().split("=");
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie_relation[0], relation);
            }

            for (Map.Entry<String, Integer> entry: map.entrySet()) {
                /*
                 * Transpose movieA and movieB for next multiplication map-reduce stage
                 *
                 * - Elton Hu (Oct.6, 2018)
                 */
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue() / sum;
                // Output: movieB\t movieA=normalizedRelationValue
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        job.setMapperClass(NormalizationMapper.class);
        job.setReducerClass(NormalizationReducer.class);

        job.setJarByClass(Normalization.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
