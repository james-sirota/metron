/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

package org.apache.metron.parsers.activedirectory;

import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.metron.common.csv.CSVConverter;
import org.apache.metron.stellar.common.utils.ConversionUtils;
import org.apache.metron.parsers.BasicParser;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveDirectoryParsingBolt extends BasicParser {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String AD_STREAM_FORMAT = "streamFormat";
  public static final String NORMALIZE_FOR_METRON = "normalizeForMetron";
  public static final String TRANSFORM_KEYS_FOR_METRON = "transformKeysForMetron";
  private transient GenericMetronADParser converter;

  
  
  @Override
  public void configure(Map<String, Object> parserConfig) {
	  
	  String streamFormat = parserConfig.get(AD_STREAM_FORMAT).toString();
	  String normalizeForMetron = parserConfig.get(NORMALIZE_FOR_METRON).toString();
	  String transformKeysForMetron = parserConfig.get(TRANSFORM_KEYS_FOR_METRON).toString();
	  
	  if(streamFormat.equals("syslog") || streamFormat == null)
		  	converter = new AdAsSyslogParser();
  		
  	  else
  		  converter = new AdAsCsvParser();
 
  		
    
  }

  @Override
  public void init() {

  }


  @Override
  public List<JSONObject> parse(byte[] rawMessage) {
    try {
      String msg = new String(rawMessage, "UTF-8");
      JSONObject jsonVal = new JSONObject();
      
      jsonVal = converter.parse(msg);
      
      if (normalizeForMetron != null && normalizeForMetron.equals("yes")) 
    	  jsonVal = ads.normalizeForMetron(obj);
		if(transformKeysForMetron != null && transformKeysForMetron.equals("yes"))
		  jsonVal = ads.transformKeysForMetron(obj);
      
      
      if(jsonVal != null) {
    	  jsonVal.put("original_string", msg);
        

        return ImmutableList.of(jsonVal);
      }
      else {
        return Collections.emptyList();
      }
    } catch (Throwable e) {
      String message = "Unable to parse " + new String(rawMessage) + ": " + e.getMessage();
      LOG.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }
}
