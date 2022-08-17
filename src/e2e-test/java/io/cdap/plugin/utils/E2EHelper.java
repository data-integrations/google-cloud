package io.cdap.plugin.utils;

import io.cdap.e2e.utils.CdfHelper;
import org.junit.Assert;

/**
 * E2E Test helper functions.
 */
public interface E2EHelper extends CdfHelper {

  default void enterPropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    try {
      String propertyLocator = CdfPluginPropertyLocator.fromPropertyString(pluginProperty).pluginProperty;
      clickMacroButton(propertyLocator);
      enterMacro(propertyLocator, macroArgument);
    } catch (NullPointerException e) {
      Assert.fail("CDF_PLUGIN_PROPERTY_MAPPING for '" + pluginProperty + "' not present in CdfPluginPropertyLocator.");
    }
  }
}
