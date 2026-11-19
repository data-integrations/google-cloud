/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.gcp.common;

import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link GCPUtils#loadServiceAccountCredentials(String, boolean)}.
 */
public class GCPUtilsTest {
  private static final String STS_URL = "https://sts.googleapis.com/v1/token";
  private static final String IMPERSONATION_URL = "https://iamcredentials.googleapis.com/v1/projects/-/"
    + "serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken";
  private static final String FAKE_SA_TOKEN = "fake-service-agent-token";

  // local stand-ins for the GCE metadata server and a non-Google token exchange endpoint
  private HttpServer metadataServer;
  private HttpServer tokenExchangeServer;
  private final AtomicInteger metadataHits = new AtomicInteger();
  private final AtomicReference<String> tokenExchangeBody = new AtomicReference<>();

  @Before
  public void setUp() throws IOException {
    metadataServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    metadataServer.createContext("/computeMetadata/v1/instance/service-accounts/default/token", exchange -> {
      metadataHits.incrementAndGet();
      byte[] body = ("{\"access_token\":\"" + FAKE_SA_TOKEN + "\",\"expires_in\":3599,\"token_type\":\"Bearer\"}")
        .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    metadataServer.start();

    tokenExchangeServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    tokenExchangeServer.createContext("/token-exchange", exchange -> {
      try (InputStream is = exchange.getRequestBody()) {
        tokenExchangeBody.set(new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
      }
      byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(500, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    tokenExchangeServer.start();
  }

  @After
  public void tearDown() {
    metadataServer.stop(0);
    tokenExchangeServer.stop(0);
  }

  @Test
  public void testServiceAccountJson() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    String pem = "-----BEGIN PRIVATE KEY-----\\n"
      + Base64.getEncoder().encodeToString(keyPairGenerator.generateKeyPair().getPrivate().getEncoded())
      + "\\n-----END PRIVATE KEY-----\\n";
    String json = "{\"type\":\"service_account\",\"project_id\":\"p\",\"private_key_id\":\"k\","
      + "\"private_key\":\"" + pem + "\",\"client_email\":\"sa@p.iam.gserviceaccount.com\",\"client_id\":\"1\"}";

    GoogleCredentials credentials = GCPUtils.loadServiceAccountCredentials(json, false);

    Assert.assertTrue(credentials instanceof ServiceAccountCredentials);
    Assert.assertNotNull(credentials.createScoped(GCPUtils.BIGQUERY_SCOPES));
  }

  @Test
  public void testExternalAccountAsGeneratedByCdapWorkloadIdentity() throws Exception {
    // shape produced by cdap-kubernetes WorkloadIdentityUtil
    String json = "{\"type\":\"external_account\",\"audience\":\"identitynamespace:pool:provider\","
      + "\"subject_token_type\":\"urn:ietf:params:oauth:token-type:jwt\","
      + "\"token_url\":\"" + STS_URL + "\","
      + "\"service_account_impersonation_url\":\"" + IMPERSONATION_URL + "\","
      + "\"credential_source\":{\"file\":\"/var/run/secrets/tokens/gcp-ksa/token\"}}";

    GoogleCredentials credentials = GCPUtils.loadServiceAccountCredentials(json, false);

    Assert.assertTrue(credentials instanceof ExternalAccountCredentials);
  }

  @Test
  public void testExternalAccountWithoutImpersonationUrl() throws Exception {
    String json = externalAccount(STS_URL, null, "{\"file\":\"/var/run/secrets/token\"}");

    GoogleCredentials credentials = GCPUtils.loadServiceAccountCredentials(json, false);

    Assert.assertTrue(credentials instanceof ExternalAccountCredentials);
  }

  @Test
  public void testExternalAccountWithUrlAndAwsSources() throws Exception {
    // credential_source is not restricted; only the endpoints tokens are sent to are
    String url = externalAccount(STS_URL, null, urlSource("http://localhost:8080/token"));
    String aws = externalAccount(STS_URL, null, "{\"environment_id\":\"aws1\","
      + "\"region_url\":\"http://169.254.169.254/latest/meta-data/placement/availability-zone\","
      + "\"url\":\"http://169.254.169.254/latest/meta-data/iam/security-credentials\","
      + "\"regional_cred_verification_url\":\"https://sts.{region}.amazonaws.com?Action=GetCallerIdentity"
      + "&Version=2011-06-15\"}");

    Assert.assertTrue(GCPUtils.loadServiceAccountCredentials(url, false) instanceof ExternalAccountCredentials);
    Assert.assertTrue(GCPUtils.loadServiceAccountCredentials(aws, false) instanceof ExternalAccountCredentials);
  }

  @Test
  public void testExternalAccountWithGcloudGeneratedEndpoints() throws Exception {
    // locational (--sts-location) and mTLS (--enable-mtls) endpoints generated by
    // 'gcloud iam workload-identity-pools create-cred-config'
    String[][] endpoints = {
      { "https://sts.us-central1.rep.googleapis.com/v1/token", null },
      { "https://sts.mtls.googleapis.com/v1/token", null },
      { "https://sts.mtls.googleapis.com/v1/token",
        "https://iamcredentials.mtls.googleapis.com/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com"
          + ":generateAccessToken" },
    };
    for (String[] endpoint : endpoints) {
      String json = externalAccount(endpoint[0], endpoint[1], "{\"file\":\"/var/run/secrets/token\"}");
      GoogleCredentials credentials = GCPUtils.loadServiceAccountCredentials(json, false);
      Assert.assertTrue(json, credentials instanceof ExternalAccountCredentials);
    }
  }

  @Test
  public void testExternalAccountRequiresGoogleTokenUrl() {
    String[] badTokenUrls = {
      "https://other.example.com/token",
      "https://sts.googleapis.com.other.example.com/v1/token",
      "https://other.example.com/?sts.googleapis.com",
      "https://user@sts.googleapis.com@other.example.com/v1/token",
      "http://sts.googleapis.com/v1/token",
      "https://sts.googleapis.com/v1/token/",
      "https://sts.googleapis.com/v1/token?x=1",
      "https://STS.googleapis.com/v1/token",
      "https://sts-myendpoint.p.googleapis.com/v1/token",
      "https://sts.other.example.com/v1/token",
      "https://sts.us-central1.rep.other.example.com/v1/token",
      "https://sts.mtls.googleapis.com.other.example.com/v1/token",
      "https://sts..rep.googleapis.com/v1/token",
      "https://sts.us-central1.googleapis.com/v1/token",
      " https://sts.googleapis.com/v1/token",
    };
    for (String tokenUrl : badTokenUrls) {
      assertRejected(externalAccount(tokenUrl, null, "{\"file\":\"/tmp/token\"}"), "token_url");
    }
    // absent, null and non-string values
    assertRejected(externalAccount(null, null, "{\"file\":\"/tmp/token\"}"), "token_url");
    assertRejected("{\"type\":\"external_account\",\"token_url\":null,\"credential_source\":{\"file\":\"/t\"}}",
                   "token_url");
    assertRejected("{\"type\":\"external_account\",\"token_url\":[\"" + STS_URL + "\"],"
                     + "\"credential_source\":{\"file\":\"/t\"}}", "token_url");
  }

  @Test
  public void testExternalAccountRequiresGoogleImpersonationUrl() {
    String iam = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";
    String[] badUrls = {
      "https://other.example.com/generateAccessToken",
      "https://iamcredentials.googleapis.com.other.example.com/v1/projects/-/serviceAccounts/x:generateAccessToken",
      "https://iamcredentials.mtls.googleapis.com.other.example.com/v1/projects/-/serviceAccounts/x"
        + ":generateAccessToken",
      "http://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/x:generateAccessToken",
      "https://iamcredentials.us-central1.rep.googleapis.com/v1/projects/-/serviceAccounts/x:generateAccessToken",
      iam + "sa@p.iam.gserviceaccount.com:generateIdToken",
      iam + "sa@p.iam.gserviceaccount.com:generateAccessToken/../../../x",
      iam + "sa@p.iam.gserviceaccount.com:generateAccessToken?x=1",
      "https://iamcredentials.googleapis.com/v1/projects/p/serviceAccounts/sa@p.iam.gserviceaccount.com"
        + ":generateAccessToken",
    };
    for (String url : badUrls) {
      assertRejected(externalAccount(STS_URL, url, "{\"file\":\"/tmp/token\"}"), "service_account_impersonation_url");
    }
    assertRejected("{\"type\":\"external_account\",\"token_url\":\"" + STS_URL + "\","
                     + "\"service_account_impersonation_url\":123,\"credential_source\":{\"file\":\"/t\"}}",
                   "service_account_impersonation_url");
  }

  /**
   * The credential configuration points the subject token source at the metadata server and the token exchange
   * at a non-Google endpoint. Loading must fail before either endpoint is contacted.
   */
  @Test
  public void testNonGoogleTokenUrlIsRejectedBeforeUse() {
    String json = externalAccount(tokenExchangeUrl(), null, urlSource(metadataUrl()));

    try {
      GoogleCredentials credentials = GCPUtils.loadServiceAccountCredentials(json, false);
      try {
        // any API call does this under the hood
        credentials.createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"))
          .refreshAccessToken();
      } catch (Exception ignored) {
        // the exchange is expected to fail, but by then the token would already have been sent
      }
      Assert.fail("Credential configuration should have been rejected");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("token_url"));
    }
    Assert.assertEquals("subject token source must not be contacted", 0, metadataHits.get());
    Assert.assertNull("nothing must be sent to the token_url", tokenExchangeBody.get());
  }

  /**
   * Variant where the token exchange goes to Google but the impersonation endpoint is not a Google endpoint.
   */
  @Test
  public void testNonGoogleImpersonationUrlIsRejected() {
    String json = externalAccount(STS_URL, tokenExchangeUrl(), urlSource(metadataUrl()));

    assertRejected(json, "service_account_impersonation_url");
    Assert.assertEquals(0, metadataHits.get());
    Assert.assertNull(tokenExchangeBody.get());
  }

  /**
   * The auth library parser is lenient and ignores content after the top level object. Validation must see the
   * same document as the library, so such content must not cause the configuration to escape validation.
   */
  @Test
  public void testTrailingContentDoesNotBypassValidation() {
    String payload = externalAccount(tokenExchangeUrl(), null, urlSource(metadataUrl()));
    for (String suffix : new String[] { " garbage", "\n{}", "]", "\u0000" }) {
      assertRejected(payload + suffix, "token_url");
    }
    Assert.assertEquals(0, metadataHits.get());
    Assert.assertNull(tokenExchangeBody.get());
  }

  @Test
  public void testOtherContentIsHandledByLibrary() {
    // behaviour for content that is not an external account configuration is unchanged, including content that
    // the library's lenient parser accepts
    for (String json : new String[] { "not json", "null", "[]", "{\"project_id\":\"no-type\"}",
      "{\"type\":\"authorized_user\"}", "{\"type\":\"authorized_user\"} trailing", "{\"type\":\"service_account\"}]",
      "{\"type\":\"service_account\",\"type\":\"authorized_user\"}" }) {
      Exception expected = loadWithLibrary(json);
      Exception actual = null;
      try {
        GCPUtils.loadServiceAccountCredentials(json, false);
      } catch (Exception e) {
        actual = e;
      }
      Assert.assertNotNull(json, expected);
      Assert.assertNotNull(json, actual);
      Assert.assertEquals(json, expected.getClass(), actual.getClass());
      Assert.assertEquals(json, expected.getMessage(), actual.getMessage());
    }
  }

  private String metadataUrl() {
    return "http://127.0.0.1:" + metadataServer.getAddress().getPort()
      + "/computeMetadata/v1/instance/service-accounts/default/token";
  }

  private String tokenExchangeUrl() {
    return "http://127.0.0.1:" + tokenExchangeServer.getAddress().getPort() + "/token-exchange";
  }

  private static Exception loadWithLibrary(String json) {
    try {
      GoogleCredentials.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
      return null;
    } catch (Exception e) {
      return e;
    }
  }

  private static String externalAccount(String tokenUrl, String impersonationUrl, String credentialSource) {
    StringBuilder json = new StringBuilder("{\"type\":\"external_account\",")
      .append("\"audience\":\"//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/x\",")
      .append("\"subject_token_type\":\"urn:ietf:params:oauth:token-type:jwt\",");
    if (tokenUrl != null) {
      json.append("\"token_url\":\"").append(tokenUrl).append("\",");
    }
    if (impersonationUrl != null) {
      json.append("\"service_account_impersonation_url\":\"").append(impersonationUrl).append("\",");
    }
    return json.append("\"credential_source\":").append(credentialSource).append("}").toString();
  }

  private static String urlSource(String url) {
    return "{\"url\":\"" + url + "\",\"headers\":{\"Metadata-Flavor\":\"Google\"},"
      + "\"format\":{\"type\":\"json\",\"subject_token_field_name\":\"access_token\"}}";
  }

  private static void assertRejected(String json, String expectedField) {
    try {
      GCPUtils.loadServiceAccountCredentials(json, false);
      Assert.fail("Expected rejection of " + json);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Invalid external account credentials"));
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("'" + expectedField + "'"));
    }
  }
}
