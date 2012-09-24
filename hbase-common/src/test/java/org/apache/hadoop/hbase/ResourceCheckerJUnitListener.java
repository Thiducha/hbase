package org.apache.hadoop.hbase;


import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.runner.notification.RunListener;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;

public class ResourceCheckerJUnitListener extends RunListener {
  private Map<String, ResourceChecker> rcs = new HashMap<String, ResourceChecker>();

  static class ThreadResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    public int getVal() {
      return Thread.getAllStackTraces().size();
    }

    public int getMax() {
      return 500;
    }
  }

  /**
   * On unix, we know how to get the number of open file descriptor
   */
  abstract static class OSResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    protected static final OperatingSystemMXBean osStats;
    protected static final UnixOperatingSystemMXBean unixOsStats;

    static {
      osStats =
          ManagementFactory.getOperatingSystemMXBean();
      if (osStats instanceof UnixOperatingSystemMXBean) {
        unixOsStats = (UnixOperatingSystemMXBean) osStats;
      } else {
        unixOsStats = null;
      }
    }
  }

  static class OpenFileDescriptorResourceAnalyzer extends OSResourceAnalyzer {
    @Override
    public int getVal() {
      if (unixOsStats == null) {
        return 0;
      } else {
        return (int) unixOsStats.getOpenFileDescriptorCount();
      }
    }

    public int getMax() {
      return 1024;
    }
  }

  static class MaxFileDescriptorResourceAnalyzer extends OSResourceAnalyzer {
    @Override
    public int getVal() {
      if (unixOsStats == null) {
        return 0;
      } else {
        return (int) unixOsStats.getMaxFileDescriptorCount();
      }
    }
  }


  public ResourceCheckerJUnitListener() {
  }

  protected void addResourceAnalyzer(ResourceChecker rc) {
  }

  /**
   * To be called before the test methods
   *
   * @param testName
   */
  private void start(String testName) {
    ResourceChecker rc = new ResourceChecker(testName);
    rc.addResourceAnalyzer(new ThreadResourceAnalyzer());
    rc.addResourceAnalyzer(new OpenFileDescriptorResourceAnalyzer());
    rc.addResourceAnalyzer(new MaxFileDescriptorResourceAnalyzer());

    addResourceAnalyzer(rc);

    rcs.put(testName, rc);

    rc.start();
  }

  /**
   * To be called after the test methods
   *
   * @param testName
   */
  private void end(String testName) {
    ResourceChecker rc = rcs.remove(testName);
    assert rc != null;
    rc.end();
  }

  /**
   * Get the test name from the JUnit Description
   *
   * @return the string for the short test name
   */
  private String descriptionToShortTestName(
      org.junit.runner.Description description) {
    final int toRemove = "org.apache.hadoop.hbase.".length();
    return description.getTestClass().getName().substring(toRemove) +
        "#" + description.getMethodName();
  }

  public void testStarted(org.junit.runner.Description description) throws java.lang.Exception {
    start(descriptionToShortTestName(description));
  }

  public void testFinished(org.junit.runner.Description description) throws java.lang.Exception {
    end(descriptionToShortTestName(description));
  }
}

