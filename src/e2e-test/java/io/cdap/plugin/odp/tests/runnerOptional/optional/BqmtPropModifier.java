package io.cdap.plugin.odp.tests.runnerOptional.optional;

import cucumber.api.event.EventListener;
import cucumber.api.event.EventPublisher;
import io.cdap.plugin.odp.utils.CDAPUtils;

import java.io.FileInputStream;
import java.io.IOException;

public class BqmtPropModifier implements EventListener {
  public BqmtPropModifier(String fileRelativePath) {
    //called in the beginning, before any scenarios are executed
    appendToProps(fileRelativePath);
  }

  private void appendToProps(String fileRelativePath) {
    System.out.println("=========================================================" + fileRelativePath);
    System.out.println(CDAPUtils.pluginProp.entrySet().size());
    try {
      CDAPUtils.pluginProp.load(new FileInputStream(fileRelativePath));
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println(CDAPUtils.pluginProp.entrySet().size());
  }

  @Override
  public void setEventPublisher(EventPublisher eventPublisher) {
    //called after all tagged scenarios are completed, for eg: reporting. As of now do nothing
  }
}
