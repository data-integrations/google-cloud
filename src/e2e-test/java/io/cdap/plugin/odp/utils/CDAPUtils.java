package io.cdap.plugin.odp.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * CDAPUtils.
 */
public class CDAPUtils {

  private static Properties errorProp = new Properties();

  static {
    try {
      errorProp.load(new FileInputStream("src/e2e-test/resources/error_message.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String pluginProp(String property) throws IOException {
    Properties prop = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream("src/e2e-test/resources/PluginParameters.properties");
      prop.load(input);
      return prop.getProperty(property);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return property;
  }

  public static boolean schemaValidation() {
    boolean schemaCreated = false;
    List<WebElement> elements = SeleniumDriver.getDriver().findElements(By.xpath("//*[@placeholder='Field name']"));
    if (elements.size() > 2) {
      schemaCreated = true;
      return schemaCreated;
    }
    return false;
  }

  public static String getErrorProp(String errorMessage) {
    return errorProp.getProperty(errorMessage);
  }
}
