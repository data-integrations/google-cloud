package io.cdap.plugin.gcp.dlp.configs;

/**
 *
 */
public class ErrorConfig {

  private String transform;
  private String fields;
  private String filters;
  private String transformPropertyId;
  private Boolean isNestedError;


  public ErrorConfig(DlpFieldTransformationConfig transformationConfig, String transformPropertyId,
    Boolean isNestedError) {
    this.transform = transformationConfig.getTransform();
    this.fields = String.join(",", transformationConfig.getFields());
    this.filters = String.join(",", transformationConfig.getFilters());
    this.transformPropertyId = transformPropertyId;
    this.isNestedError = isNestedError;
  }

  public void setTransformPropertyId(String transformPropertyId) {
    this.isNestedError = false;
    this.transformPropertyId = transformPropertyId;
  }

  public void setNestedTransformPropertyId(String transformPropertyId) {
    this.isNestedError = true;
    this.transformPropertyId = transformPropertyId;
  }
}
