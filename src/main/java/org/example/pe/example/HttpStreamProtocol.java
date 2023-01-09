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
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.util.Map;

public class HttpStreamProtocol extends PullProtocol {

    Logger logger = LoggerFactory.getLogger(HttpStreamProtocol.class);
    public static final String ID = "org.example.pe.example.datasource";
    private static final String USERNAME_PROPERTY ="username";
    private static final String PASSWORD_PROPERTY ="password";
    private static final String USER_GROUP_PROPERTY ="group";
    private static final String BASE_URL_PROPERTY ="url";
    private static final String REPOSITORY_PROPERTY ="repository";
    private static final String MODEL_PROPERTY = "model";
    private static final String SENSOR_PROPERTY = "sensor";

    private static final String INTERVAL_PROPERTY ="interval";
    //private static final String ACCESS_TOKEN_PROPERTY ="access_token";

    private String url;
    private String accessToken;

    List<JSONObject> selected_sensors = new ArrayList<>();

    public HttpStreamProtocol() {
    }


    public HttpStreamProtocol(IParser parser, IFormat format, String username, String password, String group, String base_url, String repository, String model, String sensorName, long interval) {
        super(parser, format, interval);
        //System.out.println(base_url+" "+repository+ " "+model+" "+sensorName);
        this.accessToken = login(username, password, group, base_url);
        //System.out.println(accessToken);
        this.selected_sensors = getSelectedSensors(base_url, repository, model, this.accessToken);
        System.out.println(selected_sensors);
        this.url = getUrl(this.selected_sensors, base_url, repository, model, sensorName, this.accessToken);
        System.out.println(this.url);
    }

    @Override
    public Protocol getInstance(ProtocolDescription protocolDescription, IParser parser, IFormat format) {
        ParameterExtractor extractor = new ParameterExtractor(protocolDescription.getConfig());
        //ProcessorParams parameter = new ProcessorParams((DataProcessorInvocation) protocolDescription.getConfig());

        String usernameProperty = extractor.singleValue(USERNAME_PROPERTY);
        String passwordProperty = extractor.singleValue(PASSWORD_PROPERTY);
        String groupProperty = extractor.singleValue(USER_GROUP_PROPERTY);
        String base_urlProperty = extractor.singleValue(BASE_URL_PROPERTY);
        String repositoryProperty = extractor.singleValue(REPOSITORY_PROPERTY);
        String modelProperty = extractor.singleValue(MODEL_PROPERTY);
        String sensorProperty = extractor.singleValue(SENSOR_PROPERTY);
        System.out.println(usernameProperty+" "+passwordProperty);

        try {
            long intervalProperty = Long.parseLong(extractor.singleValue(INTERVAL_PROPERTY));
            // TODO change access token to an optional parameter
        //  String accessToken = extractor.singleValue(ACCESS_TOKEN_PROPERTY);
            return new HttpStreamProtocol(parser, format, usernameProperty, passwordProperty, groupProperty, base_urlProperty, repositoryProperty, modelProperty, sensorProperty, intervalProperty);
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
                .requiredTextParameter(Labels.withId(USER_GROUP_PROPERTY))
                .requiredTextParameter(Labels.withId(BASE_URL_PROPERTY))
                .requiredTextParameter(Labels.withId(REPOSITORY_PROPERTY))
                .requiredTextParameter(Labels.withId(MODEL_PROPERTY))
                .requiredTextParameter(Labels.withId(SENSOR_PROPERTY))
                .requiredIntegerParameter(Labels.withId(INTERVAL_PROPERTY))
                //.requiredTextParameter(Labels.from(ACCESS_TOKEN_PROPERTY, "Access Token", "Http
                // Access Token"))
                .build();
    }

    @Override
    public GuessSchema getGuessSchema() throws ParseException {
        int n = 8;

        InputStream dataInputStream = getDataFromEndpoint();

        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);
        if (dataByte.size() < n) {
            logger.error("Error in HttpStreamProtocol! Required: " + n + " elements but the resource just had: " +
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
            logger.error("Error in HttpStreamProtocol! User required: " + n + " elements but the resource just had: " +
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
        System.out.println(this.url);
        try {
            Request request = Request.Get(this.url)
                    .connectTimeout(1000)
                    .socketTimeout(100000);

            if (this.accessToken != null && !this.accessToken.equals("")) {
                request.setHeader("Authorization", "Bearer " + this.accessToken);
            }

            result = request
                    .execute().returnContent().asStream();

//            if (s.startsWith("ï")) {
//                s = s.substring(3);
//            }
//            result = IOUtils.toInputStream(s, "UTF-8");

        } catch (Exception e) {
            logger.error("Error while fetching data from URL: " + url, e);
            throw new ParseException("Error while fetching data from URL: " + url);
//            throw new AdapterException();
        }
        if (result == null)
            throw new ParseException("Could not receive Data from file: " + url);

        return result;
    }





    private static String login(String user, String pass, String group, String base_url) {
        String urlString, token = null;
        try {
            urlString = base_url + "admin/token?group=" + group + "&pass=" + pass + "&user=" + user;
            if(urlString.contains(" "))
                urlString = urlString.replace(" ", "%20");
            System.out.println(urlString);
            //URL url = new URL(urlString);
            URL url = new URL("https://kyklos.jotne.com/EDMtruePLM/api/admin/token?group=sdai-group&pass=!Gen8ric&user=gkyklos");

            // Open a connection to the API endpoint
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            // Send the POST request to the API endpoint
            connection.connect();

            // Read the response from the API endpoint
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            System.out.println(response);
            String json_string = String.valueOf(response);
            // Parse the JSON string as a JSON object
            JSONObject json_object = new JSONObject(json_string);
            // Access the data in the JSON object
            token = json_object.getString("token");

        } catch (Exception e) {
            // Handle any exceptions that occur
            e.printStackTrace();
        }
        return token;
    }


    private static StringBuilder connectToPLMBackend(String urlString, String accessToken) {
        String line;
        StringBuilder response = new StringBuilder();
        //replace spaces by "%20" to avoid 400 Bad Request
        if(urlString.contains(" "))
            urlString = urlString.replace(" ", "%20");

        try {
            // Set the URL of the API endpoint
            URL url = new URL(urlString);
            // Open a connection to the API endpoint
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");
            // Set the token in the HTTP header of the request
            connection.setRequestProperty("Authorization", "Bearer" + accessToken);
            // Send the GET request to the API endpoint
            connection.connect();
            // Read the response from the API endpoint
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        } catch (Exception e) {
            // Handle any exceptions that occur
            e.printStackTrace();
        }
        return response;
    }

    private static JSONArray sensorsList(String base_url, String repository, String model,String accessToken) {
        // Set the URL of the API endpoint
        String urlString = base_url + "bkd/q_search/" + repository + "/" + model + "/" + accessToken + "?case_sens=false&domains=PROPERTY&pattern=*&folder_only=false";
        StringBuilder response = connectToPLMBackend(urlString, accessToken);
        System.out.println(response);
        // Get a String representation of the StringBuilder
        String json_array = response.toString();
        // Convert the String to a JSONArray
        return new JSONArray(json_array);
    }

    private static boolean checkIfDigit(String val_part) {
        boolean isNumber = true;
        for (int i = 0; i < val_part.length(); i++) {
            char ch = val_part.charAt(i);
            if (!Character.isDigit(ch)) {
                isNumber = false;
                break; }}        return isNumber;
    }

    private List<JSONObject> getSelectedSensors(String base_url, String repository, String model, String accessToken){
        JSONArray sensor_properties;
        JSONObject sensor, element_info, json_selected_sensor;
        String string_selected_sensor;
        List<JSONObject> selected_sensors = new ArrayList<>();

        JSONArray sensors = sensorsList(base_url, repository, model, accessToken);
        System.out.println(sensors);

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

    private String getUrl(List<JSONObject> selected_sensors, String base_url, String repository, String model, String sensorName, String accessToken) {
        String urn, urlString = null;

        for (JSONObject sensor : selected_sensors) {
            System.out.println("dentro FOR");
            if (sensor.get("name").equals(sensorName)) {
                System.out.println("Dentro IF");
                urn = sensor.getJSONArray("props").getJSONObject(0).getString("urn");
                urlString = base_url + "bkd/aggr/" + repository + "/" + model + "/" + sensor.get("id") + "/" + urn + "/" + accessToken;
                //replace spaces by "%20" to avoid 400 Bad Request
                if(urlString.contains(" "))
                    urlString = urlString.replace(" ", "%20");
                System.out.println(urlString);
                break;
            }
        }
        return urlString;
    }



}
