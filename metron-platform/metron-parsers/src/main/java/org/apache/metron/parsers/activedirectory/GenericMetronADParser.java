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
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public abstract class GenericMetronADParser {
	
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	
	protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final boolean REMOEVE_SPACES_IN_KEYS = true;
	private final boolean TRUNCATE_KEYS = true;
	private final String KEY_DELIMETER = "\\.";
	private final String KEY_GROUP_DELIMITER = ":";

	private final Map<String, String> transforms = ImmutableMap.of("SourceNetworkAddress", "ip_src_addr", "SourcePort",
			"ip_src_port", "NetworkAddress", "ip_src_addr", "Port", "ip_src_port");

	abstract JSONObject parse(String rawMessage);

	@SuppressWarnings("unchecked")
	public JSONObject normalizeForMetron(JSONObject formattedMessage) {
		JSONObject normalizedMessage = new JSONObject();

		for (Object key : formattedMessage.keySet()) {
			String newKey = key.toString().trim();

			if (newKey.endsWith(KEY_GROUP_DELIMITER)) {
				newKey = newKey.substring(0, newKey.length() - 1);
			}
			LOG.trace("trimmed group key: " + newKey);

			if (KEY_DELIMETER != null) {
				newKey = newKey.replaceAll(KEY_GROUP_DELIMITER, KEY_DELIMETER);
				LOG.trace("replaced all " + KEY_GROUP_DELIMITER + " with " + KEY_DELIMETER + " in key " + newKey);
			}

			if (REMOEVE_SPACES_IN_KEYS) {
				newKey = newKey.replaceAll(" ", "");
				LOG.trace("removed spaces in key: " + newKey);
			}

			if (TRUNCATE_KEYS) {

				String[] keyParts = newKey.split(KEY_DELIMETER);

				if (keyParts.length > 0)
					newKey = keyParts[keyParts.length - 1];

				LOG.trace("truncated key to: " + newKey);
			}

			normalizedMessage.put(newKey, formattedMessage.get(key));
		}

		LOG.trace("returning normalized message: " + normalizedMessage.toJSONString());

		return normalizedMessage;
	}

	@SuppressWarnings("unchecked")
	public JSONObject transformKeysForMetron(JSONObject formattedMessage) 
	{

		for (Entry<String, String> toReplace : transforms.entrySet()) {

			if (formattedMessage.keySet().contains(toReplace.getKey())) 
			{
				LOG.trace("Transformed field: " + toReplace.getKey() + " to " + toReplace.getValue());
				formattedMessage.put(toReplace.getValue(), formattedMessage.remove(toReplace.getKey()));
			}
		}

		LOG.trace("Returned transformed JSON: " + formattedMessage);
		return formattedMessage;
	}
	
	public String getPrettyPritedString(JSONObject jsonMessage) 
	{
		 
         JsonParser jp = new JsonParser();
         JsonElement je = jp.parse(jsonMessage.toJSONString());
         

         return gson.toJson(je);
	}

}
