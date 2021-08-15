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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class, can be utilized to generate access token and invoke Rest API's
 */
public class DataplexApiHelper {

  static final Logger LOGGER = Logger.getLogger(DataplexApiHelper.class.getName());
  static AccessToken accessToken = null;

  public String invokeDataplexApi(String urlBuilder, GoogleCredentials credentials) throws IOException {
    LOGGER.log(Level.INFO, "invokes the APIs to fetch the details {0}", urlBuilder);
    StringBuilder builder = new StringBuilder();

    URL url = new URL(urlBuilder);
    URLConnection con = url.openConnection();
    HttpURLConnection http = (HttpURLConnection) con;

    if (accessToken == null || (accessToken != null && accessToken.getExpirationTime().before(new Date()))) {
      accessToken = credentials.refreshAccessToken();
    }

    http.setRequestProperty("Authorization", "Bearer " + accessToken.getTokenValue());
    http.setRequestMethod("GET");
    http.setDoOutput(true);

    BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()));
    String line;
    while ((line = br.readLine()) != null) {
      builder.append(line);
    }

    return builder.toString();
  }
}
