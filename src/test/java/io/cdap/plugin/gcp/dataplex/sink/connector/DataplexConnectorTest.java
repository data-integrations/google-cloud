package io.cdap.plugin.gcp.dataplex.sink.connector;

import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Dataplex Connector Test. This test will only be run when below property is provided:
 * project.id -- the name of the project where staging bucket may be created.
 * It will default to active google project if you have google cloud client installed.
 * service.account.file -- the path to the service account key file
 */
public class DataplexConnectorTest {

  private static String serviceAccountFilePath;
  private static String serviceAccountKey;
  private static String project;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.

    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";

    project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    Assume.assumeFalse(String.format(messageTemplate, "project id"), project == null);
    System.setProperty("GCLOUD_PROJECT", project);

    serviceAccountFilePath = System.getProperty("service.account.file.path");
    Assume.assumeFalse(String.format(messageTemplate, "service account file path"), serviceAccountFilePath == null);

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
      StandardCharsets.UTF_8);
  }

  @Test
  public void testServiceAccountPath() throws IOException {
    DataplexConnectorConfig config =
      new DataplexConnectorConfig(project, null, serviceAccountFilePath, null);
    test(config);
  }

  @Test
  public void testServiceAccountJson() throws IOException {
    DataplexConnectorConfig config =
      new DataplexConnectorConfig(project, DataplexConnectorConfig.SERVICE_ACCOUNT_JSON, null,
        serviceAccountKey);
    test(config);
  }

  private void test(DataplexConnectorConfig config) throws IOException {
    DataplexConnector connector = new DataplexConnector(config);
    testTest(connector, config.getServiceAccountType());
    testBrowse(connector);
    testSample(connector);
    testGenerateSpec(connector);
  }

  public void testTest(DataplexConnector connector, String serviceAccountType) throws ValidationException {
    ConnectorContext mockConnectorContext = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(mockConnectorContext);
    List<ValidationFailure> validationFailures = mockConnectorContext.getFailureCollector().getValidationFailures();
    if (serviceAccountType == null) {
      assertEquals(1, validationFailures.size());
    } else {
      assertEquals(0, validationFailures.size());
    }
  }

  public void testGenerateSpec(DataplexConnector connector) {
    MockConnectorContext connectorContext = new MockConnectorContext(new MockConnectorConfigurer());
    ConnectorSpecRequest connectorSpecRequest = mock(ConnectorSpecRequest.class);
    when(connectorSpecRequest.getConnectionWithMacro())
      .thenThrow(new ValidationException(new ArrayList<>()));
    when(connectorSpecRequest.getPath()).thenReturn("Path");
    assertThrows(ValidationException.class,
      () -> connector.generateSpec(connectorContext, connectorSpecRequest));
    verify(connectorSpecRequest).getConnectionWithMacro();
    verify(connectorSpecRequest).getPath();
  }

  public void testSample(DataplexConnector connector) throws IOException {
    assertTrue(
      connector.sample(new MockConnectorContext(new MockConnectorConfigurer()), mock(SampleRequest.class))
        .isEmpty());
  }

  private void testBrowse(DataplexConnector connector) throws IOException {
    // browse project
    BrowseDetail detail = connector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
      BrowseRequest.builder("/test_path").build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals("Asset", entity.getType());
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }
  }
}
