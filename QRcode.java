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
package com.boenci.qr.processors.qrcode;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.processor.io.StreamCallback;
import java.nio.charset.Charset;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import org.apache.commons.io.IOUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.NotFoundException;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class QRcode extends AbstractProcessor {

	 	public static final PropertyDescriptor uploadPath = new PropertyDescriptor
	            .Builder().name("uploadPath")
	            .displayName("uploadPath")
	            .description("uploadPath")
	            .required(true)
	            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
	            .expressionLanguageSupported(true)
	            .build();

	    public static final Relationship Success = new Relationship.Builder()
	            .name("Success")
	            .description("On success")
	            .build();

	 	
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(uploadPath);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(Success);
        this.relationships = Collections.unmodifiableSet(relationships);
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
        if ( flowFile == null ) {
            return;
        }
		  
	  	String path = context.getProperty(uploadPath).evaluateAttributeExpressions(flowFile).getValue();
	  	
	  	 // Encoding charset
        String charset = "UTF-8";
	  	
	  	//Qr code generation purpose exapmle code
        Map<EncodeHintType, ErrorCorrectionLevel> hashMap
            = new HashMap<EncodeHintType,
                          ErrorCorrectionLevel>();
 
        hashMap.put(EncodeHintType.ERROR_CORRECTION,
                    ErrorCorrectionLevel.L);
        
        //flow file to string
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        final String contents = bytes.toString();
        
        
     // Create the QR code and save
        // in the specified folder
        // as a jpg file
        try {
        	 createQR(contents, path, charset, hashMap, 200, 200);
        }catch(WriterException | IOException f) {
        	return;
        }
        
        //writing on flow file
        flowFile = session.write(flowFile, new StreamCallback() {  
			  @Override  
			  public void process(InputStream inputStream, OutputStream outputStream) throws IOException {  
				IOUtils.write(contents, outputStream); // writes the result to the flowfile.  
			  }  
		});  
        
        session.transfer(flowFile,Success);
    }
    
    @SuppressWarnings("deprecation")
   	public static void createQR(String data, String path,
                 String charset, Map hashMap,
                 int height, int width) throws WriterException, IOException{
   			
   				BitMatrix matrix = new MultiFormatWriter().encode(
   						new String(data.getBytes(charset), charset),
   						BarcodeFormat.QR_CODE, width, height);
   				
   				MatrixToImageWriter.writeToFile(
   				matrix,
   				path.substring(path.lastIndexOf('.') + 1),
   				new File(path));
   	}
}
