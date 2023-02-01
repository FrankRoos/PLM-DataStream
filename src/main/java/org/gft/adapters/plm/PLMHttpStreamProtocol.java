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

package org.gft.adapters.plm;

import org.apache.http.client.fluent.Request;
import org.apache.streampipes.connect.adapter.guess.SchemaGuesser;
import org.apache.streampipes.connect.adapter.model.generic.Protocol;
import org.apache.streampipes.connect.api.IFormat;
import org.apache.streampipes.connect.api.IParser;
import org.apache.streampipes.connect.api.exception.ParseException;
import org.apache.streampipes.model.AdapterType;
import org.apache.streampipes.model.connect.grounding.ProtocolDescription;
import org.apache.streampipes.model.connect.guess.GuessSchema;
import org.apache.streampipes.model.schema.EventSchema;
import org.apache.streampipes.sdk.builder.adapter.ProtocolDescriptionBuilder;
import org.apache.streampipes.sdk.extractor.StaticPropertyExtractor;
import org.apache.streampipes.sdk.helpers.AdapterSourceType;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.utils.Assets;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PLMHttpStreamProtocol extends PLMPullProtocol {
    private static final long interval = 10;
    Logger logger = LoggerFactory.getLogger(PLMHttpStreamProtocol.class);
    public static final String ID = "org.gft.adapters.plm";
    PLMHttpConfig config;
    private String accessToken = null;
    List<JSONObject> selected_sensors = new ArrayList<>();

    public PLMHttpStreamProtocol() {
    }

    public PLMHttpStreamProtocol(IParser parser, IFormat format, PLMHttpConfig config) {
        super(parser, format, interval);
        this.config = config;
        this.accessToken = login();
        this.selected_sensors = getSelectedSensors();
    }

    @Override
    public ProtocolDescription declareModel() {
        return ProtocolDescriptionBuilder.create(ID)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .sourceType(AdapterSourceType.STREAM)
                .category(AdapterType.Generic)
                .requiredTextParameter(PLMHttpUtils.getUsernameLabel())
                .requiredSecret(PLMHttpUtils.getPasswordLabel())
                .requiredTextParameter(PLMHttpUtils.getModelLabel())
                .requiredTextParameter(PLMHttpUtils.getSignalLabel())
                .requiredTextParameter(PLMHttpUtils.getLowestLabel())
                .requiredTextParameter(PLMHttpUtils.getHighestLabel())
                .build();
    }


    @Override
    public Protocol getInstance(ProtocolDescription protocolDescription, IParser parser, IFormat format) {
        StaticPropertyExtractor extractor = StaticPropertyExtractor.from(protocolDescription.getConfig(), new ArrayList<>());
        PLMHttpConfig config = PLMHttpUtils.getConfig(extractor);
        return new PLMHttpStreamProtocol(parser, format, config);
    }

    @Override
    public GuessSchema getGuessSchema() throws ParseException {
        int n = 2;

        InputStream dataInputStream;
        dataInputStream = getDataFromEndpoint();

        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);
        if (dataByte.size() < n) {
            logger.error("Error in PLMHttpStreamProtocol! Required: " + n + " elements but the resource just had: " +
                    dataByte.size());

            dataByte.addAll(dataByte);
        }
        EventSchema eventSchema = parser.getEventSchema(dataByte);

        return SchemaGuesser.guessSchema(eventSchema);
    }

    @Override
    public List<Map<String, Object>> getNElements(int n) throws ParseException {
        List<Map<String, Object>> result = new ArrayList<>();

        InputStream dataInputStream;
        dataInputStream = getDataFromEndpoint();

        List<byte[]> dataByte = parser.parseNEvents(dataInputStream, n);

        // Check that result size is n. Currently just an error is logged. Maybe change to an exception
        if (dataByte.size() < n) {
            logger.error("Error in PLMHttpStreamProtocol! User required: " + n + " elements but the resource just had: " +
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
        InputStream result = null;
        if (this.accessToken == null) {
            this.accessToken = login();
        }
        String urlString = getUrl(this.selected_sensors);

        if (config.getLowestDate().compareToIgnoreCase(config.getHighestDate()) >= 0) {
            return null;
        }

        try {
            // Set the URL of the API endpoint
            URL url = new URL(urlString);
            // Open a connection to the API endpoint
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("content-type", "application/json");
            // Set the token in the HTTP header of the request
            connection.setRequestProperty("Authorization", "Bearer " + this.accessToken);
            connection.setRequestProperty("transfer-encoding", "chunked");
            connection.setRequestProperty("connection", "keep-alive");
            //connection.setDoOutput(true);
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(120000);
            // Send the GET request to the API endpoint
            connection.connect();

            if (this.accessToken != null) {
                this.accessToken = null;
            }

            result = connection.getInputStream();

        } catch (Exception e) {
            // Handle any exceptions that occur
            e.printStackTrace();
        }
        return result;
    }

    private String login() throws ParseException {
        String urlString, response, token;
        urlString = config.getBaseUrl() + "admin/token?group=" + config.getGroup() + "&pass=" + config.getPassword() + "&user=" + config.getUsername();
        if (urlString.contains(" "))
            urlString = urlString.replace(" ", "%20");

        try {
            Request request = Request.Post(urlString)
                    .connectTimeout(120000)
                    .socketTimeout(120000)
                    .setHeader("Content-Type", "application/json");

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


    private JSONArray sensorsList() throws ParseException {
        String response, urlString;
        // Set the URL of the API endpoint
        urlString = config.getBaseUrl() + "bkd/q_search/" + config.getRepository() + "/" + config.getModel() + "/" + this.accessToken + "?case_sens=false&domains=PROPERTY&folder_only=false&pattern=*";
        if (urlString.contains(" "))
            urlString = urlString.replace(" ", "%20");

        try {
            Request request = Request.Get(urlString)
                    .connectTimeout(1000)
                    .socketTimeout(240000)
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
                break;
            }
        }
        return isNumber;
    }

    private List<JSONObject> getSelectedSensors() {
        JSONArray sensor_properties;
        JSONObject sensor, element_info, json_selected_sensor;
        String string_selected_sensor;
        List<JSONObject> selected_sensors = new ArrayList<>();
        JSONArray sensors = sensorsList();

        for (int i = 0; i < sensors.length(); i++) {
            sensor = sensors.getJSONObject(i);
            element_info = sensor.getJSONObject("bkdn_elem_info");
            sensor_properties = element_info.getJSONArray("properties");
            int num_of_property = sensor_properties.length();

            if (num_of_property > 1) {
                JSONArray selected_properties = new JSONArray();
                for (int j = 0; j < num_of_property; j++) {
                    JSONObject property = sensor_properties.getJSONObject(j);
                    String[] val_parts = property.getString("val").split(" ");
                    boolean number_of_items = checkIfDigit(val_parts[0]);
                    if (val_parts.length == 2 && val_parts[1].equals("items") && number_of_items) {
                        String urn = property.getString("name");
                        if (urn.contains(":"))
                            urn = urn.replace(":", "%3A");
                        String json_string = "{\"urn\": \"" + urn + "\"," + "\"num\": " +
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

    private String getUrl(List<JSONObject> selected_sensors) {
        String urn, urlString = null;
        for (JSONObject sensor : selected_sensors) {
            if (sensor.get("name").equals(config.getSignal())) {
                urn = sensor.getJSONArray("props").getJSONObject(0).getString("urn");

                try {
                    String first_date = config.LastDateTime();
                    String second_date = config.NextDateTime();
                    urlString = config.getBaseUrl() + "bkd/aggr_exp_dt/" + config.getRepository() + "/" + config.getModel() + "/" + sensor.get("id") + "/" + urn + "/"
                            + this.accessToken + "/" + "?format=json" + "&from=" + first_date + "&to=" + second_date;
                    //replace spaces by "%20" and the two points by %3A to avoid 400 Bad Request
                    if (urlString.contains(" "))
                        urlString = urlString.replace(" ", "%20");

                } catch (java.text.ParseException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        return urlString;
    }

}
