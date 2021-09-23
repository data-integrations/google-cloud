/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.dataplex.sink.util;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.plugin.gcp.dataplex.sink.exception.ConnectorException;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;

/**
 * Helper class, can be utilized to generate access token and invoke Rest API's
 */
public class DataplexApiHelper {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DataplexApiHelper.class);
  private static AccessToken accessToken = null;

  public String invokeDataplexApi(String urlBuilder, GoogleCredentials credentials) throws ConnectorException {
    LOGGER.info("Initiating the api call to fetch the details");
    StringBuilder builder = new StringBuilder();
    try {

      URL url = new URL(urlBuilder);
      URLConnection con = url.openConnection();
      HttpURLConnection http = (HttpURLConnection) con;

      if (accessToken == null || (accessToken != null && accessToken.getExpirationTime().before(new Date()))) {
        accessToken = credentials.refreshAccessToken();
      }

      http.setRequestProperty("Authorization", "Bearer " + accessToken.getTokenValue());
      http.setRequestMethod("GET");
      http.setDoOutput(true);

      int responseCode = http.getResponseCode();
      if (responseCode == 400 || responseCode == 401 || responseCode == 402 || responseCode == 403 ||
        responseCode == 404 || responseCode == 500
        || responseCode == 501 || responseCode == 502 || responseCode == 503) {
        throw new ConnectorException(String.valueOf(responseCode), http.getResponseMessage());
      }

      BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        builder.append(line);
      }
    } catch (IOException e) {
      LOGGER.debug("Dataplex api call failed due to error: {}", e.getMessage());
    }
    return builder.toString();
  }
}
