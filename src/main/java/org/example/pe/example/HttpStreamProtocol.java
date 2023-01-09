/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.example.pe.example;

import org.apache.http.client.fluent.Request;
import org.apache.streampipes.connect.api.IParser;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.utils.Assets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.streampipes.connect.api.exception.ParseException;
import org.apache.streampipes.connect.adapter.guess.SchemaGuesser;
import org.apache.streampipes.connect.api.IFormat;
import org.apache.streampipes.connect.adapter.model.generic.Protocol;
import org.apache.streampipes.connect.adapter.sdk.ParameterExtractor;
import org.apache.streampipes.model.AdapterType;
import org.apache.streampipes.model.connect.grounding.ProtocolDescription;
import org.apache.streampipes.model.connect.guess.GuessSchema;
import org.apache.streampipes.model.schema.EventSchema;
import org.apache.streampipes.sdk.builder.adapter.ProtocolDescriptionBuilder;
import org.apache.streampipes.sdk.helpers.AdapterSourceType;
import org.apache.streampipes.sdk.helpers.Labels;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.util.Map;

public class HttpStreamProtocolPLM extends PullProtocol {
    Logger logger = LoggerFactory.getLogger(HttpStreamProtocolPLM.class);
    public static final String ID = "org.example.pe.example.datasourceplm";
    private static final String USERNAME_PROPERTY ="username";
    private static final String PASSWORD_PROPERTY ="password";
    private static final String USER_GROUP_PROPERTY ="group";
    private static final String BASE_URL_PROPERTY ="url";
    private static final String REPOSITORY_PROPERTY ="repository";
    private static final String MODEL_PROPERTY = "model";
    private static final String SENSOR_PROPERTY = "sensor";
    private static final String INTERVAL_PROPERTY ="interval";
    private String url;
    private String accessToken;
    List<JSONObject> selected_sensors = new ArrayList<>();

    public HttpStreamProtocolPLM() {
    }

    public HttpStreamProtocolPLM(IParser parser, IFormat format, String username, String password, String group, String base_url, String repository, String model, String sensorName, long interval) {
        super(parser, format, interval);
        this.accessToken = login(username, password, group, base_url);
        this.selected_sensors = getSelectedSensors(base_url, repository, model);
        this.url = getUrl(this.selected_sensors, base_url, repository, model, sensorName);
    }

    @Override
    public Protocol getInstance(ProtocolDescription protocolDescription, IParser parser, IFormat format) {
        ParameterExtractor extractor = new ParameterExtractor(protocolDescription.getConfig());

        String user = extractor.singleValue(USERNAME_PROPERTY);
        String pass = extractor.singleValue(PASSWORD_PROPERTY);
        String group = extractor.singleValue(USER_GROUP_PROPERTY);
        String base_url = extractor.singleValue(BASE_URL_PROPERTY);
        String repository = extractor.singleValue(REPOSITORY_PROPERTY);
        String model = extractor.singleValue(MODEL_PROPERTY);
        String sensor = extractor.singleValue(SENSOR_PROPERTY);

        try {
            long intervalProperty = Long.parseLong(extractor.singleValue(INTERVAL_PROPERTY));
            // TODO change access token to an optional parameter
            //  String accessToken = extractor.singleValue(ACCESS_TOKEN_PROPERTY);
            return new HttpStreamProtocolPLM(parser, format, user, pass, group, base_url, repository, model, sensor, intervalProperty);
        } catch (NumberFormatException e) {
            logger.error("Could not parse" + extractor.singleValue(INTERVAL_PROPERTY) + "to int");
            return null;
        }

    }

    @Override
    public ProtocolDescription declareModel() {
        return ProtocolDescriptionBuilder.create(ID)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .sourceType(AdapterSourceType.STREAM)
                .category(AdapterType.Generic)
                .requiredTextParameter(Labels.withId(USERNAME_PROPERTY))
                .requiredTextParameter(Labels.withId(PASSWORD_PROPERTY))
                .requiredTextParameter(Labels.withId(USER_GROUP_PROPERTY), "sdai-group")
                .requiredTextParameter(Labels.withId(BASE_URL_PROPERTY), "https://kyklos.jotne.com/EDMtruePLM/api/")
                .requiredTextParameter(Labels.withId(REPOSITORY_PROPERTY), "TruePLMprojectsRep")
                .requiredTextParameter(Labels.withId(MODEL_PROPERTY))
                .requiredTextParameter(Labels.withId(SENSOR_PROPERTY))
                .requiredIntegerParameter(Labels.withId(INTERVAL_PROPERTY))
                .build();
    }

    @Override
    public GuessSchema getGuessSchema() throws ParseException {
        int n = 8;
        InputStream dataInputStream = getDataFromEndpoint();

        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);
        if (dataByte.size() < n) {
            logger.error("Error in HttpStreamProtocolPLM! Required: " + n + " elements but the resource just had: " +
                    dataByte.size());
            dataByte.addAll(dataByte);
        }
        EventSchema eventSchema= parser.getEventSchema(dataByte);
        return SchemaGuesser.guessSchema(eventSchema);
    }

    @Override
    public List<Map<String, Object>> getNElements(int n) throws ParseException {
        List<Map<String, Object>> result = new ArrayList<>();
        InputStream dataInputStream = getDataFromEndpoint();
        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);

        // Check that result size is n. Currently just an error is logged. Maybe change to an exception
        if (dataByte.size() < n) {
            logger.error("Error in HttpStreamProtocolPLM! User required: " + n + " elements but the resource just had: " +
                    dataByte.size());
        }
        for (byte[] b : dataByte) {
            result.add(format.parse(b));
        }
        return result;
    }


    @Override
    public String getId() {
        return ID;
    }

    @Override
    public InputStream getDataFromEndpoint() throws ParseException {
        InputStream result;
        try {
            Request request = Request.Get(url)
                    .connectTimeout(1000)
                    .socketTimeout(100000)
                    .setHeader("Content-Type", "application/json");

            if (this.accessToken != null && !this.accessToken.equals("")) {
                request.setHeader("Authorization", "Bearer " + this.accessToken);
            }
            result = request
                    .execute().returnContent().asStream();
        } catch (Exception e) {
            logger.error("Error while fetching data from URL: " + url, e);
            throw new ParseException("Error while fetching data from URL: " + url);
        }
        if (result == null)
            throw new ParseException("Could not receive Data from file: " + url);
        return result;
    }

    private String login(String user, String pass, String group, String base_url) throws ParseException{
        String urlString, response, token;
        urlString = base_url + "admin/token?group=" + group + "&pass=" + pass + "&user=" + user;
        if(urlString.contains(" "))
            urlString = urlString.replace(" ", "%20");

        try {
            Request request = Request.Post(urlString)
                    .connectTimeout(1000)
                    .socketTimeout(100000)
                    .setHeader("Content-Type", "application/json");

            if (this.accessToken != null && !this.accessToken.equals("")) {
                request.setHeader("Authorization", "Bearer " + this.accessToken);
            }
            response = request
                    .execute().returnContent().toString();
            if (response == null)
                throw new ParseException("Could not receive Data from file: " + urlString);
            // Parse the JSON string as a JSON object
            JSONObject json_object = new JSONObject(response);
            // Access the data in the JSON object
            token = json_object.getString("token");

        } catch (Exception e) {
            logger.error("Error while fetching data from URL: " + urlString, e);
            throw new ParseException("Error while fetching data from URL: " + urlString);
        }
        return token;
    }


    private JSONArray sensorsList(String base_url, String repository, String model) throws ParseException{
        String response, urlString;
        // Set the URL of the API endpoint
        urlString = base_url + "bkd/q_search/" + repository + "/" + model + "/" + this.accessToken + "?case_sens=false&domains=PROPERTY&folder_only=false&pattern=*";
        if(urlString.contains(" "))
            urlString = urlString.replace(" ", "%20");

        try {
            Request request = Request.Get(urlString)
                    .connectTimeout(1000)
                    .socketTimeout(100000)
                    .setHeader("Content-Type", "application/json");

            if (this.accessToken != null && !this.accessToken.equals("")) {
                request.setHeader("Authorization", "Bearer " + this.accessToken);
            }
            response = request
                    .execute().returnContent().toString();
            if (response == null)
                throw new ParseException("Could not receive Data from file: " + urlString);

        } catch (Exception e) {
            logger.error("Error while fetching data from URL: " + urlString, e);
            throw new ParseException("Error while fetching data from URL: " + urlString);
        }
        return new JSONArray(response);
    }

    private static boolean checkIfDigit(String val_part) {
        boolean isNumber = true;
        for (int i = 0; i < val_part.length(); i++) {
            char ch = val_part.charAt(i);
            if (!Character.isDigit(ch)) {
                isNumber = false;
                break; }}        return isNumber;
    }

    private List<JSONObject> getSelectedSensors(String base_url, String repository, String model){
        JSONArray sensor_properties;
        JSONObject sensor, element_info, json_selected_sensor;
        String string_selected_sensor;
        List<JSONObject> selected_sensors = new ArrayList<>();
        JSONArray sensors = sensorsList(base_url, repository, model);

        for (int i = 0; i < sensors.length(); i++) {
            sensor = sensors.getJSONObject(i);
            element_info = sensor.getJSONObject("bkdn_elem_info");
            sensor_properties = element_info.getJSONArray("properties");
            int num_of_property = sensor_properties.length();

            if(num_of_property >1){
                JSONArray selected_properties = new JSONArray();
                for(int j = 0; j < num_of_property; j++){
                    JSONObject property = sensor_properties.getJSONObject(j);
                    String[] val_parts = property.getString("val").split(" ");
                    boolean number_of_items = checkIfDigit(val_parts[0]);
                    if(val_parts.length == 2 && val_parts[1].equals("items") && number_of_items){
                        String json_string = "{\"urn\": \"" + property.get("name") + "\"," + "\"num\": " +
                                Integer.parseInt(val_parts[0]) + "}";
                        JSONObject json_object = new JSONObject(json_string);
                        selected_properties.put(json_object);
                    }
                }
                string_selected_sensor = "{\"name\": \"" + element_info.get("name") + "\"," + "\"id\": \"" +
                        element_info.get("instance_id") + "\"," + "\"props\": " + selected_properties + "}";
                json_selected_sensor = new JSONObject(string_selected_sensor);
                selected_sensors.add(json_selected_sensor);
            }
        }
        return selected_sensors;
    }

    private String getUrl(List<JSONObject> selected_sensors, String base_url, String repository, String model, String sensorName) {
        String urn, urlString = null;

        for (JSONObject sensor : selected_sensors) {
            if (sensor.get("name").equals(sensorName)) {
                urn = sensor.getJSONArray("props").getJSONObject(0).getString("urn");
                urlString = base_url + "bkd/aggr/" + repository + "/" + model + "/" + sensor.get("id") + "/" + urn + "/" + this.accessToken + "/"+"?format=json"; //&_=1672309152628
                //replace spaces by "%20" to avoid 400 Bad Request
                if(urlString.contains(" "))
                    urlString = urlString.replace(" ", "%20");
                break;
            }
        }        return urlString;
    }

}
