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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdAsSyslogParser extends GenericMetronADParser{
	protected final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyyy");

	private final String LOG_FRAGMENT_DELIMETER = "\t";
	private final String INTERNAL_SPLITTING_DELIMETER = "\\|";
	private final String HEADER_SPLIT_DELIMITER = "\\s+";
	private final String KEY_GROUP_DELIMITER = ":";
	private final int SYSLOG_HEADER_OFFSET = 4;

	
	public AdAsSyslogParser()
	{
		LOG.info("Initialized Syslog AD Parser");
	}

	@SuppressWarnings("unchecked")
	public JSONObject parse(String unparsedString) {
		LOG.debug("Received message: " + unparsedString);

		JSONObject formattedMessage = new JSONObject();
		String[] logFragments = unparsedString.split(LOG_FRAGMENT_DELIMETER);

		formattedMessage = parseSyslogHeader(logFragments[0].substring(SYSLOG_HEADER_OFFSET));

		formattedMessage.put("syslogMessageType", Long.parseLong(logFragments[1].trim()));
		LOG.trace("syslogMessageType: " + Long.parseLong(logFragments[1].trim()));

		formattedMessage.put("syslogMessageCategory", logFragments[2].trim());
		LOG.trace("syslogMessageCategory: " + logFragments[2].trim());

		try {

			formattedMessage.put("timestamp", df.parse(logFragments[4].trim()).getTime());
			LOG.trace("timestamp: " + df.parse(logFragments[4].trim()).getTime());

		} catch (ParseException e) {
			LOG.error("Could not parse the syslog header");
			e.printStackTrace();
		}

		formattedMessage.put("auditProcess", logFragments[6].trim());
		LOG.trace("syslogMessageCategory: " + logFragments[6].trim());

		formattedMessage.put("auditType", logFragments[8].trim());
		LOG.trace("syslogMessageCategory: " + logFragments[8].trim());

		formattedMessage.put("eventType", logFragments[11].trim());
		LOG.trace("syslogMessageCategory: " + logFragments[11].trim());

		System.out.println(logFragments[13]);

		LOG.debug("raw AD message is: " + logFragments[13]);
		String delimitedAdMessage = logFragments[13].replaceAll("   ", INTERNAL_SPLITTING_DELIMETER)
				.replaceAll(": ", ":" + INTERNAL_SPLITTING_DELIMETER).replaceAll("  ", INTERNAL_SPLITTING_DELIMETER);

		LOG.trace("delimited AD message is: " + delimitedAdMessage);

		formattedMessage.putAll(parseMessage(delimitedAdMessage));
		LOG.trace("parsed message as JSON raw: " + formattedMessage.toJSONString());

		

		LOG.debug("returning a parsed message: " + formattedMessage.toJSONString());
		return formattedMessage;
	}

	@SuppressWarnings("unchecked")
	private JSONObject parseSyslogHeader(String header) {
		JSONObject headerJson = new JSONObject();

		String[] headerParts = header.split(HEADER_SPLIT_DELIMITER);
		String timestamp = headerParts[0] + " " + headerParts[1] + " " + headerParts[2] + " " + headerParts[2];
		LOG.trace("syslog timestamp extracted is: " + timestamp);

		String host = headerParts[3];
		String logSource = headerParts[4];

		headerJson.put("syslogTimestamp", timestamp);
		LOG.trace("syslogTimestamp: " + timestamp);

		headerJson.put("syslogHost", host);
		LOG.trace("syslogHost: " + host);

		headerJson.put("logSource", logSource);
		LOG.trace("logSource: " + logSource);

		LOG.debug("returning header JSON: " + headerJson);

		return headerJson;
	}

	@SuppressWarnings("unchecked")
	private JSONObject parseMessage(String rawMessage) {

		JSONObject formattedMessage = new JSONObject();
		rawMessage = rawMessage.trim();

		String terms[] = rawMessage.split(INTERNAL_SPLITTING_DELIMETER);
		String keyGroup = "";

		for (int i = 1; i < terms.length; i++) {
			terms[i] = terms[i].trim();
			LOG.trace("Extracted term: " + terms[i]);

			while (terms[i].endsWith(KEY_GROUP_DELIMITER) && (i < terms.length - 1)) {
				LOG.trace("found a group term: " + terms[i]);

				keyGroup = keyGroup + terms[i];
				LOG.trace("the group key is now: " + keyGroup);

				i = i + 1;
				LOG.trace("term index incremented to: " + i);
			}

			LOG.trace("group key is : " + keyGroup + " for term " + terms[i]);

			if (formattedMessage.containsKey(keyGroup)) {
				formattedMessage.put(keyGroup, formattedMessage.get(keyGroup) + " " + terms[i].trim());
				LOG.trace("appending to key : " + keyGroup + " for term " + terms[i]);
			} else {
				formattedMessage.put(keyGroup, terms[i].trim());
				LOG.trace("inserting to key : " + keyGroup + " for term " + terms[i]);
			}

			keyGroup = reAssmbleKey(keyGroup);
			LOG.trace("group changed to : " + keyGroup);

		}

		LOG.trace("Returning formatted AD message : " + keyGroup);
		return formattedMessage;

	}

	private String reAssmbleKey(String oldKey) {
		String[] parts = oldKey.split(KEY_GROUP_DELIMITER);
		String newKey = parts[0] + KEY_GROUP_DELIMITER;

		for (int i = 1; i < parts.length - 1; i++)
			newKey = newKey + parts[i] + KEY_GROUP_DELIMITER;

		LOG.trace("Group key reassambled to: " + newKey);

		return newKey;

	}



}
