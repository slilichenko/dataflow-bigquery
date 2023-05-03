/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.bigquery.ingestion;

import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.util.Iterator;

/**
 * Data provider which reads a Json new line delimited file and return an iterator of Json objects.
 */
public class JsonNewLineDelimitedFileDataProvider implements Closeable {
    private BufferedReader reader;
    private String nextLine;

    public JsonNewLineDelimitedFileDataProvider(String file) throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(file));
    }

    public Iterator<JSONObject> getData() {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                try {
                    nextLine = reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return nextLine != null;
            }

            @Override
            public JSONObject next() {
                return new JSONObject(new JSONTokener(nextLine));
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
