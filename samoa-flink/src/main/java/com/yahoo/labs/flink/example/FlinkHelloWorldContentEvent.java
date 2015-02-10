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

import java.util.ArrayList;
import java.util.List;


public class FlinkHelloWorldContentEvent implements ContentEvent {

    private boolean isLastEvent;
    private int helloWorldData;
    private int processorsIds = 0;
    boolean alreadyProcessed = false;

    public FlinkHelloWorldContentEvent(int helloWorldData, boolean isLastEvent) {
        this.isLastEvent = isLastEvent;
        this.helloWorldData = helloWorldData;
    }

    /*
     * No-argument constructor for Kryo
     */
    public FlinkHelloWorldContentEvent() {
        this(0, false);
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public void setKey(String str) {
        // do nothing, it's key-less content event
    }

    @Override
    public boolean isLastEvent() {
        return isLastEvent;
    }


    public void setIsLastEvent(boolean isLastEvent) {
        this.isLastEvent = isLastEvent;
    }

    public void setHelloWorldData(int helloWorldData) {
        this.helloWorldData = helloWorldData;
    }

    public int getHelloWorldData() {
        return helloWorldData;
    }

    public int getProcessorsIds() {
        return processorsIds;
    }

    public void addPIinProcessorsIds(){
        this.processorsIds++;
    }

    public boolean isAlreadyProcessed() {
        return alreadyProcessed;
    }

    public void setAlreadyProcessed(boolean alreadyProcessed) {
        this.alreadyProcessed = alreadyProcessed;
    }

    @Override
    public String toString() {
        return "HelloWorldContentEvent [helloWorldData=" + helloWorldData + "]";
    }
}