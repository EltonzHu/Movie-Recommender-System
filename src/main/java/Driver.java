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

public class Driver {
    public static void main(String[] args) throws Exception {

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Normalization normalization = new Normalization();
        UnitMultiplication unitMultiplication = new UnitMultiplication();
        UnitSum unitSum = new UnitSum();

        /*
         * Input parameters parsing:
         * args[0]: Raw data-set
         * args[1]: Output directory for DataDividerByUser job
         * args[2]: Output directory for CoOccurrenceMatrixGenerator job
         * args[3]: Output directory for Normalization job
         * args[4]: Output directory for UnitMultiplication job
         * args[5]: Output directory for UnitSum job
         *
         * - Elton Hu (Oct.7, 2018)
         */

        String rawInput = args[0];
        String userMoviesOutputDir = args[1];
        String coOccurrenceMatrixOutputDir = args[2];
        String normalizationOutputDir = args[3];
        String unitMultiplicatonOutputDir = args[4];
        String unitSumOutputDir = args[5];

        String[] input1 = {rawInput, userMoviesOutputDir};
        String[] input2 = {userMoviesOutputDir, coOccurrenceMatrixOutputDir};
        String[] input3 = {coOccurrenceMatrixOutputDir, normalizationOutputDir};
        String[] input4 = {normalizationOutputDir, rawInput, unitMultiplicatonOutputDir};
        String[] input5 = {unitMultiplicatonOutputDir, unitSumOutputDir};

        dataDividerByUser.main(input1);
        coOccurrenceMatrixGenerator.main(input2);
        normalization.main(input3);
        unitMultiplication.main(input4);
        unitSum.main(input5);
    }
}
