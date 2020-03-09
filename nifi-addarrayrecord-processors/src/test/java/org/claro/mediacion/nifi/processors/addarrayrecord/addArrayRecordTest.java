/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.claro.mediacion.nifi.processors.addarrayrecord;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class addArrayRecordTest {

    private TestRunner runner;
    @Before
    public void setup() {
        runner = TestRunners.newTestRunner( AddArrayRecord.class );
    }

    @Test
    public void process() throws InitializationException, IOException {
        CSReader();
        CSWriter();
        setProperty();
        runner.clearTransferState();
        runner.enqueue( Paths.get( "src/test/inFlowFile"));
        runner.run();

        final String expectedOutput = new String( Files.readAllBytes(Paths.get("src/test/expected")));
        runner.getFlowFilesForRelationship( AddArrayRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);

    }
    @Test
    public void reprocess() throws InitializationException, IOException {
        CSReader();
        CSWriter();
        setProperty();
        runner.clearTransferState();
        runner.enqueue( Paths.get( "src/test/inFlowFileRep"));
        runner.run();

        final String expectedOutput = new String( Files.readAllBytes(Paths.get("src/test/expectedRep")));
        runner.getFlowFilesForRelationship( AddArrayRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);

    }

    private void CSReader () throws InitializationException, IOException {
        //Reader
        final JsonTreeReader jsonReader = new JsonTreeReader();
        //final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/inschema.avsc")));
        runner.addControllerService( "reader", jsonReader );
        //runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        //runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService( jsonReader );
    }
    private void CSWriter() throws InitializationException, IOException {
        //Writer
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        //final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/outschema.avsc")));
        runner.addControllerService("writer", jsonWriter);
        //runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        //runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        //runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
    }
    private void setProperty (){
        runner.setProperty( AddArrayRecord.RECORD_READER, "reader" );
        runner.setProperty( AddArrayRecord.RECORD_WRITER, "writer" );
        runner.setProperty( AddArrayRecord.ARRAY_RECORD, "E_Formatted,E_Error_Code,E_Formatted_Date,E_Error_Description");
        runner.setProperty( AddArrayRecord.FIELD, "HISTORY");
    }
}

