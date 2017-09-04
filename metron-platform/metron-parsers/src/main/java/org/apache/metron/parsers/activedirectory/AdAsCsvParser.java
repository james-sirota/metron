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

import java.lang.invoke.MethodHandles;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdAsCsvParser extends GenericMetronADParser {

	private final String CSV_DELIMITER = ",";
	private final String FIELD_SEPARATOR_VALUE = "\\\\r\\\\n";
	private DateTimeFormatter parser = ISODateTimeFormat.dateTime();
	protected final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	public AdAsCsvParser()
	{
		LOG.info("Initialized CSV AD Parser");
	}

	public JSONObject parse(String unparsedString) 
	{
		LOG.debug("Received message: " + unparsedString);

		String[] message = unparsedString.split(CSV_DELIMITER);
		LOG.trace("Attempting to parse CSV with delimiter: " + CSV_DELIMITER);

		for (String message_part : message)
			LOG.trace("Extracted:" + message_part);

		String timestamp = message[0].trim();
		LOG.trace("Raw timestamp value is: " + timestamp);

		JSONObject formattedMessage = new JSONObject();

		Long epochTimestamp = parser.parseDateTime(timestamp).getMillis();
		LOG.trace("Converted epoch timestamp value is: " + epochTimestamp);

		formattedMessage.put("timestamp", epochTimestamp);
		LOG.trace("Value of JSON at this point:" + formattedMessage.toJSONString());

		String[] terms = message[1].split(FIELD_SEPARATOR_VALUE);

		for (String term : terms)
			LOG.trace("Extracted CSV value terms" + term);

		formattedMessage.put("event", terms[0].trim());
		LOG.trace("Value of JSON at this point:" + formattedMessage.toJSONString());

		String keyGroup = "";

		for (int i = 1; i < terms.length; i++) {

			terms[i] = terms[i].trim();
			LOG.trace("Analyzing term[" + i + "" + terms[i]);

			if (terms[i] != null && terms[i].compareTo("") != 0)
				LOG.trace("The following term is null, nothing to do, skipping:" + terms[i]);

			if (terms[i].endsWith(":")) {
				LOG.trace("Found a top-level group, setting keyGroup to:" + terms[i]);

				keyGroup = terms[i];
				keyGroup = keyGroup.substring(0, keyGroup.length() - 1);

			}

			else if (terms[i].contains(":")) {

				LOG.trace("Found a key-value term:" + terms[i]);

				String vals[] = terms[i].split(":");
				vals[1] = vals[1].trim().replaceAll("\\\"", "");

				LOG.trace("Key is:" + vals[0]);
				LOG.trace("Value is:" + vals[1]);

				if (keyGroup != null) {
					LOG.trace("Found pre-existing key group:" + keyGroup);
					formattedMessage.put(keyGroup + "." + vals[0], vals[1].trim());
					LOG.trace("Value of JSON at this point:" + formattedMessage.toJSONString());
				} else {
					LOG.trace("No pre-existing key group found");
					formattedMessage.put(vals[0], vals[1].trim());
					LOG.trace("Value of JSON at this point:" + formattedMessage.toJSONString());
				}
			} else {
				if (!formattedMessage.containsKey("message")) {
					LOG.trace("Setting a new message");
					formattedMessage.put("message", terms[i].trim());
					LOG.trace("Value of JSON at this point:" + formattedMessage.toJSONString());
				} else {
					LOG.trace("Appending to a new message");
					formattedMessage.put("message", (formattedMessage.get("message") + " " + terms[i]).trim());
					LOG.trace("Value of JSON at this point:" + formattedMessage.toJSONString());
				}

				LOG.trace("Resetting the keyGroup");
				keyGroup = null;

			}

		}

		LOG.debug("Parsed message is:" + formattedMessage);

		return formattedMessage;
	}

}
