package io.cdap.plugin.gcp.dataplex.sink.connection;

import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.plugin.gcp.dataplex.sink.connection.out.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;
import io.cdap.plugin.gcp.dataplex.sink.util.DataplexApiHelper;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DataplexInterfaceImpl.class)
public class DataplexInterfaceTest {

  @InjectMocks
  DataplexInterfaceImpl dataplexInterface = new DataplexInterfaceImpl();

  @Mock
  GoogleCredentials googleCredentials;

  DataplexApiHelper dataplexApiHelper;

  @Before
  public void setUp() throws Exception {
    dataplexApiHelper = PowerMockito.mock(DataplexApiHelper.class);
    PowerMockito.whenNew(DataplexApiHelper.class).withAnyArguments().thenReturn(dataplexApiHelper);
  }

  @Test
  public void listLocationsTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getLocations());
    List<Location> locations = dataplexInterface.listLocations(googleCredentials, "");
    assertEquals(7, locations.size());
    assertEquals("australia-southeast1", locations.get(1).locationId);
  }

  @Test
  public void getLocationTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getLocation());
    Location location = dataplexInterface.getLocation(googleCredentials, "", "");
    assertEquals("us-central1", location.locationId);
  }

  @Test
  public void listLakesTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getLakes());
    List<Lake> lakes = dataplexInterface.listLakes(googleCredentials, "", "");
    assertEquals(1, lakes.size());
    assertEquals("example lake", lakes.get(0).getDisplayName());
  }

  @Test
  public void getLakeTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getLake());
    Lake lake = dataplexInterface.getLake(googleCredentials, "", "", "");
    assertEquals("example lake", lake.getDisplayName());
  }

  @Test
  public void listZonesTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getZones());
    List<Zone> zones = dataplexInterface.listZones(googleCredentials, "", "", "");
    assertEquals(1, zones.size());
    assertEquals("example zone", zones.get(0).displayName);
  }

  @Test
  public void getZoneTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getZone());
    Zone zone = dataplexInterface.getZone(googleCredentials, "", "", "", "");
    assertEquals("example zone", zone.displayName);
  }

  @Test
  public void listAssetsTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getAssets());
    List<Asset> assets = dataplexInterface.listAssets(googleCredentials, "", "", "", "");
    assertEquals(3, assets.size());
    assertEquals("test2", assets.get(0).displayName);
  }

  @Test
  public void getAssetTest() throws Exception {
    when(dataplexApiHelper.invokeDataplexApi(anyString(), any(GoogleCredentials.class))).thenReturn(getAsset());
    Asset asset = dataplexInterface.getAsset(googleCredentials, "", "", "", "", "");
    assertEquals("Storage Asset", asset.displayName);
  }

  private String getLocations() {
    return "{\n" +
      "    \"locations\": [\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/asia-northeast1\",\n" +
      "            \"locationId\": \"asia-northeast1\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/australia-southeast1\",\n" +
      "            \"locationId\": \"australia-southeast1\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/europe-west2\",\n" +
      "            \"locationId\": \"europe-west2\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-central1\",\n" +
      "            \"locationId\": \"us-central1\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-east1\",\n" +
      "            \"locationId\": \"us-east1\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-east4\",\n" +
      "            \"locationId\": \"us-east4\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-west1\",\n" +
      "            \"locationId\": \"us-west1\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
  }

  private String getLocation() {
    return "{\n" +
      "    \"name\": \"projects/sap-adapter/locations/us-central1\",\n" +
      "    \"locationId\": \"us-central1\"\n" +
      "}";
  }

  private String getLakes() {
    return "{\n" +
      "    \"lakes\": [\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake\",\n" +
      "            \"displayName\": \"example lake\",\n" +
      "            \"uid\": \"6fff5a56-4d70-4945-b7ca-861cb2d96e15\",\n" +
      "            \"createTime\": \"2021-08-05T06:25:45.183933755Z\",\n" +
      "            \"updateTime\": \"2021-08-05T06:28:58.315739968Z\",\n" +
      "            \"state\": \"ACTIVE\",\n" +
      "            \"serviceAccount\": \"service-662869350220@gcp-sa-dataplex.iam.gserviceaccount.com\",\n" +
      "            \"securitySpec\": {},\n" +
      "            \"securityStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-12T07:02:30.854462Z\"\n" +
      "            },\n" +
      "            \"metastore\": {},\n" +
      "            \"assetStatus\": {\n" +
      "                \"updateTime\": \"2021-08-12T07:02:30.847357Z\",\n" +
      "                \"activeAssets\": 3\n" +
      "            }\n" +
      "        }\n" +
      "    ]\n" +
      "}";
  }

  private String getLake() {
    return "{\n" +
      "    \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake\",\n" +
      "    \"displayName\": \"example lake\",\n" +
      "    \"uid\": \"6fff5a56-4d70-4945-b7ca-861cb2d96e15\",\n" +
      "    \"createTime\": \"2021-08-05T06:25:45.183933755Z\",\n" +
      "    \"updateTime\": \"2021-08-05T06:28:58.315739968Z\",\n" +
      "    \"state\": \"ACTIVE\",\n" +
      "    \"serviceAccount\": \"service-662869350220@gcp-sa-dataplex.iam.gserviceaccount.com\",\n" +
      "    \"securitySpec\": {},\n" +
      "    \"securityStatus\": {\n" +
      "        \"state\": \"READY\",\n" +
      "        \"updateTime\": \"2021-08-12T07:02:30.854462Z\"\n" +
      "    },\n" +
      "    \"metastore\": {},\n" +
      "    \"assetStatus\": {\n" +
      "        \"updateTime\": \"2021-08-12T07:02:30.847357Z\",\n" +
      "        \"activeAssets\": 3\n" +
      "    }\n" +
      "}";
  }

  private String getZones() {
    return "{\n" +
      "    \"zones\": [\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake/zones/example-zone\",\n" +
      "            \"displayName\": \"example zone\",\n" +
      "            \"uid\": \"d970a06e-1318-4f3d-8cbb-e2915f8895cd\",\n" +
      "            \"createTime\": \"2021-08-06T05:45:17.757925731Z\",\n" +
      "            \"updateTime\": \"2021-08-06T05:45:20.817468749Z\",\n" +
      "            \"state\": \"ACTIVE\",\n" +
      "            \"type\": \"RAW\",\n" +
      "            \"securitySpec\": {},\n" +
      "            \"securityStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-12T07:02:11.156945Z\"\n" +
      "            },\n" +
      "            \"discoverySpec\": {\n" +
      "                \"enabled\": true,\n" +
      "                \"schedule\": \"0 * * * *\",\n" +
      "                \"publishing\": {\n" +
      "                    \"metastore\": {\n" +
      "                        \"enabled\": true,\n" +
      "                        \"databaseName\": \"exaple_lake_example_zone\"\n" +
      "                    },\n" +
      "                    \"bigquery\": {\n" +
      "                        \"enabled\": true,\n" +
      "                        \"datasetName\": \"projects/sap-adapter/datasets/exaple_lake_example_zone\"\n" +
      "                    }\n" +
      "                }\n" +
      "            },\n" +
      "            \"resourceSpec\": {\n" +
      "                \"locationType\": \"SINGLE_REGION\"\n" +
      "            },\n" +
      "            \"assetStatus\": {\n" +
      "                \"updateTime\": \"2021-08-12T07:02:11.156137Z\",\n" +
      "                \"activeAssets\": 3\n" +
      "            }\n" +
      "        }\n" +
      "    ]\n" +
      "}";
  }

  public String getZone() {
    return "{\n" +
      "    \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake/zones/example-zone\",\n" +
      "    \"displayName\": \"example zone\",\n" +
      "    \"uid\": \"d970a06e-1318-4f3d-8cbb-e2915f8895cd\",\n" +
      "    \"createTime\": \"2021-08-06T05:45:17.757925731Z\",\n" +
      "    \"updateTime\": \"2021-08-06T05:45:20.817468749Z\",\n" +
      "    \"state\": \"ACTIVE\",\n" +
      "    \"type\": \"RAW\",\n" +
      "    \"securitySpec\": {},\n" +
      "    \"securityStatus\": {\n" +
      "        \"state\": \"READY\",\n" +
      "        \"updateTime\": \"2021-08-12T07:02:11.156945Z\"\n" +
      "    },\n" +
      "    \"discoverySpec\": {\n" +
      "        \"enabled\": true,\n" +
      "        \"schedule\": \"0 * * * *\",\n" +
      "        \"publishing\": {\n" +
      "            \"metastore\": {\n" +
      "                \"enabled\": true,\n" +
      "                \"databaseName\": \"exaple_lake_example_zone\"\n" +
      "            },\n" +
      "            \"bigquery\": {\n" +
      "                \"enabled\": true,\n" +
      "                \"datasetName\": \"projects/sap-adapter/datasets/exaple_lake_example_zone\"\n" +
      "            }\n" +
      "        }\n" +
      "    },\n" +
      "    \"resourceSpec\": {\n" +
      "        \"locationType\": \"SINGLE_REGION\"\n" +
      "    },\n" +
      "    \"assetStatus\": {\n" +
      "        \"updateTime\": \"2021-08-12T07:02:11.156137Z\",\n" +
      "        \"activeAssets\": 3\n" +
      "    }\n" +
      "}";
  }

  public String getAssets() {
    return "{\n" +
      "    \"assets\": [\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake/zones/" +
      "example-zone/assets/test2\",\n" +
      "            \"displayName\": \"test2\",\n" +
      "            \"uid\": \"326a8799-ed50-401a-b8ca-59fd9d8c6fff\",\n" +
      "            \"createTime\": \"2021-08-12T06:35:37.140619650Z\",\n" +
      "            \"updateTime\": \"2021-08-12T06:36:13.261467611Z\",\n" +
      "            \"description\": \"test \",\n" +
      "            \"state\": \"ACTIVE\",\n" +
      "            \"resourceSpec\": {\n" +
      "                \"name\": \"projects/sap-adapter/datasets/test_sap_cdf_stagiing\",\n" +
      "                \"type\": \"BIGQUERY_DATASET\",\n" +
      "                \"creationPolicy\": \"ATTACH_RESOURCE\",\n" +
      "                \"deletionPolicy\": \"DETACH_RESOURCE\"\n" +
      "            },\n" +
      "            \"resourceStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-12T07:05:43.225228Z\"\n" +
      "            },\n" +
      "            \"securitySpec\": {},\n" +
      "            \"securityStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-12T06:35:42.610651Z\"\n" +
      "            },\n" +
      "            \"discoverySpec\": {\n" +
      "                \"enabled\": true,\n" +
      "                \"schedule\": \"TZ=Asia/Anadyr 0 * * * *\"\n" +
      "            },\n" +
      "            \"discoveryStatus\": {\n" +
      "                \"state\": \"SCHEDULED\",\n" +
      "                \"updateTime\": \"2021-08-12T07:00:31.220712Z\",\n" +
      "                \"lastRunTime\": \"2021-08-12T07:00:30.143074411Z\",\n" +
      "                \"nextRunTime\": \"2021-08-12T08:00:00Z\",\n" +
      "                \"stats\": {},\n" +
      "                \"lastRunDuration\": \"0.484839272s\"\n" +
      "            }\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake/zones/" +
      "example-zone/assets/storage-asset\",\n" +
      "            \"displayName\": \"Storage Asset\",\n" +
      "            \"uid\": \"f03be43d-f5f1-4a41-845a-5fe9b9d44582\",\n" +
      "            \"createTime\": \"2021-08-10T17:12:07.368343202Z\",\n" +
      "            \"updateTime\": \"2021-08-10T17:12:43.075043683Z\",\n" +
      "            \"state\": \"ACTIVE\",\n" +
      "            \"resourceSpec\": {\n" +
      "                \"name\": \"projects/_/buckets/dataplex_bucket\",\n" +
      "                \"type\": \"STORAGE_BUCKET\",\n" +
      "                \"creationPolicy\": \"ATTACH_RESOURCE\",\n" +
      "                \"deletionPolicy\": \"DETACH_RESOURCE\"\n" +
      "            },\n" +
      "            \"resourceStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-12T07:03:23.726386Z\"\n" +
      "            },\n" +
      "            \"securitySpec\": {},\n" +
      "            \"securityStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-10T17:12:11.857382Z\"\n" +
      "            },\n" +
      "            \"discoverySpec\": {\n" +
      "                \"enabled\": true,\n" +
      "                \"schedule\": \"0 * * * *\"\n" +
      "            },\n" +
      "            \"discoveryStatus\": {\n" +
      "                \"state\": \"SCHEDULED\",\n" +
      "                \"updateTime\": \"2021-08-12T07:03:23.726386Z\",\n" +
      "                \"lastRunTime\": \"2021-08-12T07:02:57.830013854Z\",\n" +
      "                \"nextRunTime\": \"2021-08-12T08:00:00Z\",\n" +
      "                \"stats\": {},\n" +
      "                \"lastRunDuration\": \"19.611240146s\"\n" +
      "            }\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake/zones/" +
      "example-zone/assets/example-asset\",\n" +
      "            \"displayName\": \"example asset\",\n" +
      "            \"uid\": \"a12bd2e4-7826-43ef-8efd-e43c3dac1c7d\",\n" +
      "            \"createTime\": \"2021-08-09T08:26:24.644593795Z\",\n" +
      "            \"updateTime\": \"2021-08-09T08:27:00.992535440Z\",\n" +
      "            \"state\": \"ACTIVE\",\n" +
      "            \"resourceSpec\": {\n" +
      "                \"name\": \"projects/sap-adapter/datasets/exaple_lake_example_zone\",\n" +
      "                \"type\": \"BIGQUERY_DATASET\",\n" +
      "                \"creationPolicy\": \"ATTACH_RESOURCE\",\n" +
      "                \"deletionPolicy\": \"DETACH_RESOURCE\"\n" +
      "            },\n" +
      "            \"resourceStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-12T07:00:31.125544Z\"\n" +
      "            },\n" +
      "            \"securitySpec\": {},\n" +
      "            \"securityStatus\": {\n" +
      "                \"state\": \"READY\",\n" +
      "                \"updateTime\": \"2021-08-09T08:26:29.781631Z\"\n" +
      "            },\n" +
      "            \"discoverySpec\": {\n" +
      "                \"enabled\": true,\n" +
      "                \"schedule\": \"TZ=America/Costa_Rica 0 * * * *\"\n" +
      "            },\n" +
      "            \"discoveryStatus\": {\n" +
      "                \"state\": \"SCHEDULED\",\n" +
      "                \"updateTime\": \"2021-08-12T07:00:31.125544Z\",\n" +
      "                \"lastRunTime\": \"2021-08-12T07:00:30.112059887Z\",\n" +
      "                \"nextRunTime\": \"2021-08-12T08:00:00Z\",\n" +
      "                \"stats\": {},\n" +
      "                \"lastRunDuration\": \"0.588266944s\"\n" +
      "            }\n" +
      "        }\n" +
      "    ]\n" +
      "}";
  }

  public String getAsset() {
    return "{\n" +
      "    \"name\": \"projects/sap-adapter/locations/us-central1/lakes/exaple-lake/zones/" +
      "example-zone/assets/storage-asset\",\n" +
      "    \"displayName\": \"Storage Asset\",\n" +
      "    \"uid\": \"f03be43d-f5f1-4a41-845a-5fe9b9d44582\",\n" +
      "    \"createTime\": \"2021-08-10T17:12:07.368343202Z\",\n" +
      "    \"updateTime\": \"2021-08-10T17:12:43.075043683Z\",\n" +
      "    \"state\": \"ACTIVE\",\n" +
      "    \"resourceSpec\": {\n" +
      "        \"name\": \"projects/_/buckets/dataplex_bucket\",\n" +
      "        \"type\": \"STORAGE_BUCKET\",\n" +
      "        \"creationPolicy\": \"ATTACH_RESOURCE\",\n" +
      "        \"deletionPolicy\": \"DETACH_RESOURCE\"\n" +
      "    },\n" +
      "    \"resourceStatus\": {\n" +
      "        \"state\": \"READY\",\n" +
      "        \"updateTime\": \"2021-08-12T07:03:23.726386Z\"\n" +
      "    },\n" +
      "    \"securitySpec\": {},\n" +
      "    \"securityStatus\": {\n" +
      "        \"state\": \"READY\",\n" +
      "        \"updateTime\": \"2021-08-10T17:12:11.857382Z\"\n" +
      "    },\n" +
      "    \"discoverySpec\": {\n" +
      "        \"enabled\": true,\n" +
      "        \"schedule\": \"0 * * * *\"\n" +
      "    },\n" +
      "    \"discoveryStatus\": {\n" +
      "        \"state\": \"SCHEDULED\",\n" +
      "        \"updateTime\": \"2021-08-12T07:03:23.726386Z\",\n" +
      "        \"lastRunTime\": \"2021-08-12T07:02:57.830013854Z\",\n" +
      "        \"nextRunTime\": \"2021-08-12T08:00:00Z\",\n" +
      "        \"stats\": {},\n" +
      "        \"lastRunDuration\": \"19.611240146s\"\n" +
      "    }\n" +
      "}";
  }

}
