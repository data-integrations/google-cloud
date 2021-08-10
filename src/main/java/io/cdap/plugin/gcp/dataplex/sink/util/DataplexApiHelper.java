package io.cdap.plugin.gcp.dataplex.sink.util;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * Helper class, can be utilized to generate access token and contains util methods to invoke Rest API's
 */
public class DataplexApiHelper {

  static final Logger LOGGER = Logger.getLogger(DataplexApiHelper.class.getName());

  /**
   * Generates AccessToken based on the json provided
   *
   * @return accessToken as String
   */
  static String getAccessToken(String filePath) {
    LOGGER.info(MessageFormat.format("generates the access token based on the service account provided", filePath));
    GoogleCredentials credentials = null;
    AccessToken token = null;
    try {
      credentials =
        GoogleCredentials.fromStream(new FileInputStream(filePath))
          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
    } catch (Exception e) {
      LOGGER.info(MessageFormat.format("unable to create the credentials", e));
    }
    try {
      token = credentials.refreshAccessToken();
    } catch (IOException e) {
      LOGGER.info(MessageFormat.format("unable to generate the access token", e));
    }
    return token.getTokenValue();
  }

  public String invokeDataplexApi(String urlBuilder, String filePath) {
    LOGGER.info(MessageFormat.format("invokes the API's to fetch the details - ", urlBuilder));
    URL url = null;
    StringBuilder builder = new StringBuilder();
    try {
      url = new URL(urlBuilder);

      URLConnection con = url.openConnection();
      HttpURLConnection http = (HttpURLConnection) con;
      http.setRequestProperty("Authorization", "Bearer " + getAccessToken(filePath));
      http.setRequestMethod("GET");
      http.setDoOutput(true);

      BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        builder.append(line);
      }
    } catch (Exception e) {
      LOGGER.info(MessageFormat.format("Unable to invoke the API for the URL - {0} and the exception is {1}",
        urlBuilder, e));
    }
    return builder.toString();
  }
}
