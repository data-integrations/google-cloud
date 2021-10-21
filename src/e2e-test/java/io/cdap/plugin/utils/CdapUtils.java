package io.cdap.plugin.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * CdapUtils contains the helper functions.
 */
public class CdapUtils {

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
}
