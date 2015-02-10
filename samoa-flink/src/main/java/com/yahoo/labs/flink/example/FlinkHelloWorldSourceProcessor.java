package com.yahoo.labs.flink.example;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2015 Yahoo! Inc.
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

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.core.Processor;

import java.util.Random;


public class FlinkHelloWorldSourceProcessor implements EntranceProcessor {

    private Random rnd = new Random();
    private final long maxInst;
    private long count = 0;

    public FlinkHelloWorldSourceProcessor(long maxInst) {
        this.maxInst = maxInst;
    }

    @Override
    public boolean process(ContentEvent event) {
        // do nothing, API will be refined further
        return false;
    }

    @Override
    public void onCreate(int id) {
        rnd = new Random(id);
    }

    @Override
    public Processor newProcessor(Processor p) {
        FlinkHelloWorldSourceProcessor hwsp = (FlinkHelloWorldSourceProcessor) p;
        return new FlinkHelloWorldSourceProcessor(hwsp.maxInst);
    }

    @Override
    public boolean isFinished() {
        return count >= maxInst;
    }

    @Override
    public boolean hasNext() {
        return count < maxInst;
    }

    @Override
    public ContentEvent nextEvent() {
        if (count%2==0){
            count++;
            return new FlinkHelloWorldContentEvent(rnd.nextInt(), false);
        }
        else
        {
            count++;
            return new FlinkContentEvent(("My Content Event "+ String.valueOf(count)),false);
        }
    }
}
