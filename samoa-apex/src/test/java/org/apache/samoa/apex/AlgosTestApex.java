package org.apache.samoa.apex;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2016 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.samoa.TestParams;
import org.apache.samoa.TestUtils;
import org.apache.samoa.apex.LocalApexDoTask;
import org.junit.Test;

public class AlgosTestApex {

  @Test(timeout = 600000)
  public void testVHTWithApex() throws Exception {

    TestParams vhtConfig = new TestParams.Builder()
        .inputInstances(200_000)
        .samplingSize(20_000)
        .evaluationInstances(200_000)
        .classifiedInstances(200_000)
        .labelSamplingSize(10l)
        .classificationsCorrect(55f)
        .kappaStat(0f)
        .kappaTempStat(0f)
        .cliStringTemplate(TestParams.Templates.PREQEVAL_VHT_RANDOMTREE)
        .resultFilePollTimeout(30)
        .prePollWait(15)
        .taskClassName(LocalApexDoTask.class.getName())
        .build();
    TestUtils.test(vhtConfig);

  }

  @Test(timeout = 120000)
  public void testNaiveBayesWithApex() throws Exception {
    TestParams baggingConfig = new TestParams.Builder()
        .inputInstances(200_000)
        .samplingSize(20_000)
        .evaluationInstances(180_000)
        .classifiedInstances(190_000)
        .labelSamplingSize(10l)
        .classificationsCorrect(60f)
        .kappaStat(0f)
        .kappaTempStat(0f)
        .cliStringTemplate(TestParams.Templates.PREQEVAL_NAIVEBAYES_HYPERPLANE)
        .resultFilePollTimeout(40)
        .prePollWait(20)
        .taskClassName(LocalApexDoTask.class.getName())
        .build();
    TestUtils.test(baggingConfig);

  }
}
