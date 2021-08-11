package io.cdap.plugin.gcp.dataplex.sink.util;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Helper class, can be utilized to generate access token and contains util methods to invoke Rest API's
 */
public class DataplexApiHelper {

  static final Logger LOGGER = Logger.getLogger(DataplexApiHelper.class.getName());
  static AccessToken accessToken = null;

  public String invokeDataplexApi(String urlBuilder, GoogleCredentials credentials) throws IOException {
    LOGGER.info(MessageFormat.format("invokes the API's to fetch the details - {0}", urlBuilder));
    URL url = null;
    StringBuilder builder = new StringBuilder();

    url = new URL(urlBuilder);
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
