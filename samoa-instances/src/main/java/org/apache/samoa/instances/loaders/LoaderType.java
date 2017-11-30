package org.apache.samoa.instances.loaders;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2017 Apache Software Foundation
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


public enum LoaderType {
    AVRO_BINARY_LOADER("AVRO_BINARY_LOADER"),
    AVRO_JSON_LOADER("AVRO_JSON_LOADER"),
    JSON_LOADER("JSON_LOADER"),
    ARFF_LOADER("ARFF_LOADER"),
    UNKNOWN("UNKNOWN");

    private String loaderName;

    LoaderType(String loaderName){

    }

    public static LoaderType fromString(String str) {
        for (LoaderType v :
                LoaderType.values()) {
            if (v.toString().equalsIgnoreCase(str)) return v;
        }
        return UNKNOWN;
    }
}
