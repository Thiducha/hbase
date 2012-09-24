package org.apache.hadoop.hbase;


import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.runner.notification.RunListener;

import java.lang.management.ManagementFactory;

public class ResourceJUnitListener extends RunListener {
  private String processId = ManagementFactory.getRuntimeMXBean().getName();
  private ResourceChecker cu;
  private boolean endDone;

  public ResourceJUnitListener() {
    System.err.println("AAAAAAAAAAAAAAAA ResourceJUnitListener  ");
  }

  /**
   * To be called before the test methods
   *
   * @param testName
   */
  private void start(String testName) {
    cu = new ResourceChecker("before " + testName);
    endDone = false;

  }

  /**
   * To be called after the test methods
   *
   * @param testName
   */
  private void end(String testName) {
    if (!endDone) {
      endDone = true;
      cu.logInfo("after " + testName);
      cu.check("after " + testName);
    }
  }

  /**
   * Get the test name from the JUnit Description
   *
   * @param description
   * @return the string for the short test name
   */
  private String descriptionToShortTestName(
      org.junit.runner.Description description) {
    final int toRemove = "org.apache.hadoop.hbase.".length();
    return description.getTestClass().getName().substring(toRemove) +
        "#" + description.getMethodName();
  }

  public void testRunStarted(org.junit.runner.Description description) throws java.lang.Exception {
    System.err.println("AAAAAAAAAAAAAAAA testRunStarted  " + ManagementFactory.getRuntimeMXBean().getName());
  }

  public void testRunFinished(org.junit.runner.Result result) throws java.lang.Exception {
    System.err.println("AAAAAAAAAAAAAAAAA testRunFinished  " + ManagementFactory.getRuntimeMXBean().getName());

  }

  public void testStarted(org.junit.runner.Description description) throws java.lang.Exception {
    System.err.println("AAAAAAAAAAAAAAAA testStarted  " + ManagementFactory.getRuntimeMXBean().getName());
    start(descriptionToShortTestName(description));
  }

  public void testFinished(org.junit.runner.Description description) throws java.lang.Exception {
    System.err.println("AAAAAAAAAAAAAAAA testFinished  " + ManagementFactory.getRuntimeMXBean().getName());
    end(descriptionToShortTestName(description));
  }

  public void testFailure(org.junit.runner.notification.Failure failure) throws java.lang.Exception {
    System.err.println("AAAAAAAAAAAAAAAA testFailure  " + ManagementFactory.getRuntimeMXBean().getName());
  }

  public void testAssumptionFailure(org.junit.runner.notification.Failure failure) {
    System.err.println("AAAAAAAAAAAAAAAA testAssumptionFailure  " + ManagementFactory.getRuntimeMXBean().getName());

  }

  public void testIgnored(org.junit.runner.Description description) throws java.lang.Exception {
    System.err.println("AAAAAAAAAAAAAAAA testIgnored  ");
  }
}

