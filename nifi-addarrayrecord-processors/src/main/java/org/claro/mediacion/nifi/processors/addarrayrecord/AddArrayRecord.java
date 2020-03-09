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

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;

import java.io.OutputStream;
import java.util.*;
import java.lang.Object;

import org.apache.nifi.serialization.record.util.DataTypeUtils;

@Tags({"history", "claro", "mediación", "record", "json","array","Ivan","Zeraus"})
@CapabilityDescription("Procesador que genera un nuevo Field con los campos existentes que se le pasan en Array Record")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class AddArrayRecord extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name( "Reader" )
            .displayName( "Record Reader" )
            .identifiesControllerService( RecordReaderFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name( "Writer" )
            .displayName( "Record Writer" )
            .identifiesControllerService( RecordSetWriterFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor FIELD = new PropertyDescriptor.Builder()
            .name( "Field" )
            .displayName( "Field" )
            .description( "Nombre del campo que se van a contener ae array record" )
            .required( true )
            .defaultValue( "HISTORY" )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .build();
    static final PropertyDescriptor ARRAY_RECORD = new PropertyDescriptor.Builder()
            .name( "Array Record" )
            .displayName( "Array Record" )
            .description( "Nombres de los campos existentes que se van a agregar al array record, separados por coma" )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .required( true )
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name( "success" )
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name( "failure" )
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add( RECORD_READER );
        descriptors.add( RECORD_WRITER );
        descriptors.add( FIELD );
        descriptors.add( ARRAY_RECORD );
        this.descriptors = Collections.unmodifiableList( descriptors );

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add( REL_SUCCESS );
        relationships.add( REL_FAILURE );
        this.relationships = Collections.unmodifiableSet( relationships );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final String arrayRecord = context.getProperty(ARRAY_RECORD).getValue();
        final String field = context.getProperty(FIELD).getValue();
        final ComponentLog logger = getLogger();
        final FlowFile outFlowFile = session.create(flowFile);
        try {
            session.read(flowFile, in -> {
                try (RecordReader reader = factory.createRecordReader(flowFile, in, logger)) {
                    Record record;
                    final OutputStream outStream = session.write(outFlowFile);

                    final List<HashMap<String, String>> listOfMaps = new ArrayList<HashMap<String, String>>();
                    final HashMap<String, String> values = new HashMap<>();
                    final String[] fieldsArray = arrayRecord.split(",");

                    record = reader.nextRecord();//firstRecord

                    // We want to transform the first record before creating the Record Writer. We do this because the Record will likely end up with a different structure
                    // and therefore a difference Schema after being transformed. As a result, we want to transform the Record and then provide the transformed schema to the
                    // Record Writer so that if the Record Writer chooses to inherit the Record Schema from the Record itself, it will inherit the transformed schema, not the
                    // schema determined by the Record Reader.

                    if (record != null) {
                        final RecordSchema writeSchema;

                        if (!record.getSchema().getField(field).isPresent()) {
                            getLogger().debug("History field is not present");
                            Record newRecordScheme = createNewSchemeRecord(fieldsArray, field, record, listOfMaps, values);
                            writeSchema = writerFactory.getSchema(null, newRecordScheme.getSchema());
                        } else {
                            writeSchema = writerFactory.getSchema(null, record.getSchema());
                        }
                        RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, outStream);
                        writer.beginRecordSet();

                        while (record != null) {
                            listOfMaps.clear();
                            values.clear();
                            createHistory(fieldsArray, record, listOfMaps, values, record.getValue(field));
                            record.setValue(field, listOfMaps);
                            writer.write(record);
                            record = reader.nextRecord();
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        writer.close();

                        final Map<String, String> attributes = new HashMap<>();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        session.putAllAttributes(outFlowFile, attributes);
                        session.transfer(outFlowFile, REL_SUCCESS);
                    } else {
                        throw new ProcessException("No content");
                    }
                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    getLogger().error("SchemaNotFound or MalformedRecordException \n" + e.toString());
                    throw new ProcessException(e);
                } catch (final Exception e) {
                    getLogger().error(e.toString());
                    throw new ProcessException(e);
                }
            });
        } catch (final Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e});
            return;
        }
        session.remove(flowFile);
    }

    private final Record createNewSchemeRecord ( final String[] fieldsArray, final String field, final Record record,
                                                 final List<HashMap<String, String>> listOfMaps,
                                                 final HashMap<String, String> values) {
        final Map<String, Object> recordMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject( record, RecordFieldType.RECORD.getRecordDataType( record.getSchema() ) );
        for (String s : fieldsArray) {
            values.put( s, null );
        }
        listOfMaps.add( values );
        recordMap.put( field, listOfMaps );
        final Record updatedRecord = DataTypeUtils.toRecord( recordMap, "r" );
        return updatedRecord;
    }

    private void createHistory ( final String[] fields, Record record,
                                 final List<HashMap<String, String>> listOfMaps,
                                 final HashMap<String, String> values, Object obj){
        getLogger().debug( "Creating history..." );
        if (obj instanceof Object[] && ((Object[]) obj).length >= 1) {
            getLogger().debug( "History array present in the registry..." );
            final Object[] arrayObj = (Object[]) obj;
            for (Object o : arrayObj) {
                final List<String> tmpList = ((Record) o).getSchema().getFieldNames();
                final HashMap<String, String> tmp = new HashMap<>();
                for (final String key : tmpList) {
                    tmp.put( key, ((Record) o).getAsString( key ) );
                }
                listOfMaps.add( tmp );
            }
        }
        //current value mapping
        for (String s : fields) {
            values.put( s, record.getValue( s ).toString() );
        }
        listOfMaps.add( values );
    }
}

