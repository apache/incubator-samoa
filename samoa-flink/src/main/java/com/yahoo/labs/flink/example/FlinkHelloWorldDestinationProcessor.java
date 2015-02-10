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
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.Stream;


public class FlinkHelloWorldDestinationProcessor implements Processor {

    private Stream outputStream;
    private Stream splitStream;
    private Stream stringOutputStream;
    private boolean isLastProc = false;
    private boolean toSplit = false;

    public FlinkHelloWorldDestinationProcessor() {
        super();
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof FlinkHelloWorldContentEvent) {
            ((FlinkHelloWorldContentEvent) event).addPIinProcessorsIds();
            System.out.println("ContentEvent key: " + event.toString() + "\tpis: " + ((FlinkHelloWorldContentEvent) event).getProcessorsIds());
            if (!isLastProc()) {
                if (isToSplit() && !((FlinkHelloWorldContentEvent) event).isAlreadyProcessed()) {
                    ((FlinkHelloWorldContentEvent) event).setAlreadyProcessed(true);
                    this.splitStream.put(event);
                } else {
                    this.outputStream.put(event);
                }
            }
        } else {
            ((FlinkContentEvent) event).addPIinProcessorsIds();
            System.err.println("ContentEvent key: " + event.toString() + "\tpis: " + ((FlinkContentEvent) event).getProcessorsIds());
            if (!isLastProc()) {
                if (isToSplit() && !((FlinkContentEvent) event).isAlreadyProcessed()) {
                    ((FlinkContentEvent) event).setAlreadyProcessed(true);
                    this.splitStream.put(event);
                } else {
                    this.stringOutputStream.put(event);
                }
            }
        }
        return true;
    }

    @Override
    public void onCreate(int id) {    }
    @Override
    public Processor newProcessor(Processor p) {
        return new FlinkHelloWorldDestinationProcessor();
    }

    public Stream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }

    public boolean isLastProc() {
        return isLastProc;
    }

    public void setLastProc(boolean isLastProc) {
        this.isLastProc = isLastProc;
    }

    public boolean isToSplit() {
        return toSplit;
    }

    public void setToSplit(boolean toSplit) {
        this.toSplit = toSplit;
    }

    public Stream getSplitStream() {
        return splitStream;
    }

    public void setSplitStream(Stream splitStream) {
        this.splitStream = splitStream;
    }

    public Stream getStringOutputStream() {
        return stringOutputStream;
    }

    public void setStringOutputStream(Stream stringOutputStream) {
        this.stringOutputStream = stringOutputStream;
    }
}