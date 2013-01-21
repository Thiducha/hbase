/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClusterManager.CommandProvider.Operation;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Shell;

/**
 * A default cluster manager for HBase. Uses SSH, and hbase shell scripts
 * to manage the cluster. Assumes Unix-like commands are available like 'ps',
 * 'kill', etc. Also assumes the user running the test has enough "power" to start & stop
 * servers on the remote machines (for example, the test user could be the same user as the
 * user the daemon isrunning as)
 */
@InterfaceAudience.Private
public class HBaseClusterManager extends ClusterManager {

  /**
   * Executes commands over SSH
   */
  static class RemoteShell extends Shell.ShellCommandExecutor {

    private String hostname;

    private String sshCmd = "/usr/bin/ssh";
    private String sshOptions = System.getenv("HBASE_SSH_OPTS"); //from conf/hbase-env.sh

    public RemoteShell(String hostname, String[] execString, File dir, Map<String, String> env,
                       long timeout) {
      super(execString, dir, env, timeout);
      this.hostname = hostname;
    }

    public RemoteShell(String hostname, String[] execString, File dir, Map<String, String> env) {
      super(execString, dir, env);
      this.hostname = hostname;
    }

    public RemoteShell(String hostname, String[] execString, File dir) {
      super(execString, dir);
      this.hostname = hostname;
    }

    public RemoteShell(String hostname, String[] execString) {
      super(execString);
      this.hostname = hostname;
    }

    @Override
    public String[] getExecString() {
      return new String[]{
          "bash", "-c",
          StringUtils.join(new String[]{sshCmd,
              sshOptions == null ? "" : sshOptions,
              hostname,
              "\"" + StringUtils.join(super.getExecString(), " ") + "\""
          }, " ")};
    }

    @Override
    public void execute() throws IOException {
      super.execute();
    }

    public void setSshCmd(String sshCmd) {
      this.sshCmd = sshCmd;
    }

    public void setSshOptions(String sshOptions) {
      this.sshOptions = sshOptions;
    }

    public String getSshCmd() {
      return sshCmd;
    }

    public String getSshOptions() {
      return sshOptions;
    }
  }

  /**
   * Provides command strings for services to be executed by Shell. CommandProviders are
   * pluggable, and different deployments(windows, bigtop, etc) can be managed by
   * plugging-in custom CommandProvider's or ClusterManager's.
   */
  static abstract class CommandProvider {

    enum Operation {
      START, STOP, RESTART
    }

    public abstract String getCommand(ServiceType service, Operation op);

    public String isRunningCommand(ServiceType service) {
      return findPidCommand(service);
    }

    protected String findPidCommand(ServiceType service) {
      return String.format("ps aux | grep proc_%s | grep -v grep | tr -s ' ' | cut -d ' ' -f2",
          service);
    }

    public String signalCommand(ServiceType service, String signal) {
      return String.format("%s | xargs kill -s %s", findPidCommand(service), signal);
    }
  }

  static String getITestDirectory() {
    return System.getenv("HOME") + Path.SEPARATOR + "tmp-recotest";
  }

  /**
   * We want everybody to use the same java dir, this allows to test multiple Java versions
   */
  static String getJavaHome() {
    return System.getenv("JAVA_HOME");
  }

  static String getHadoopVersion() {
    return System.getenv("HADOOP_VERSION");
  }

  /**
   * CommandProvider to manage the service using bin/hbase-* scripts
   */
  static class HBaseShellCommandProvider extends CommandProvider {
    private String getHBaseHome() {
      return getITestDirectory() + Path.SEPARATOR + "hbase";
    }

    private String getConfig() {
      return getHBaseHome() + Path.SEPARATOR + "conf";
    }

    @Override
    public String getCommand(ServiceType service, Operation op) {
      String cmd = "";
      cmd += "export HBASE_OPTS=-Djava.net.preferIPv4Stack=true;";
      cmd += "export JAVA_HOME=" + getJavaHome() + ";";
      cmd += "export HADOOP_SSH_OPTS='-A';";
      cmd += "export HBASE_HEAPSIZE=500;";
      cmd += "export HBASE_CONF_DIR=" + getConfig() + ";";
      cmd += "export HBASE_HOME=" + getHBaseHome() + ";";
      return cmd + String.format("%s/bin/hbase-daemon%s.sh %s  %s",
          getHBaseHome(),
          service.equals(ServiceType.ZOOKEEPER) ? "s" : "",
          op.toString().toLowerCase(), service);
    }
  }


  /**
   * CommandProvider to manage the service using bin/hbase-* scripts
   */
  static class HadoopShellCommandProvider extends CommandProvider {
    private String getHadoopHome() {
      return getITestDirectory();
    }

    private String getConfig() {
      return getHadoopHome() + Path.SEPARATOR + "conf" + Path.SEPARATOR + "conf-hadoop";
    }

    private String getHadoopCommonHome() {
      if (getHadoopVersion().startsWith("1")) {
        return getHadoopHome() + Path.SEPARATOR + "hadoop-common/build/hadoop-" +
            getHadoopVersion();
      } else {
        return getHadoopHome() + Path.SEPARATOR +
            "hadoop-common/hadoop-common-project/hadoop-common/target/hadoop-common-" +
            getHadoopVersion();
      }
    }

    private String getHDFSHome() {
      if (getHadoopVersion().startsWith("1")) {
        return getHadoopCommonHome();
      } else {
        return getHadoopHome() + Path.SEPARATOR +
            "hadoop-common/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-" +
            getHadoopVersion();
      }
    }

    @Override
    public String getCommand(ServiceType service, Operation op) {
      return getCommand(service, op.toString());
    }

    public String getCommand(ServiceType service, String op) {
      String cmd = "";
      cmd += "export JAVA_HOME=/opt/jdk1.6;";
      cmd += "export HADOOP_COMMON_HOME=" + getHadoopCommonHome() + ";";
      cmd += "export HADOOP_HDFS_HOME=" + getHDFSHome() + ";";

      if ("START".equals(op)) {
        op = null;
      }

      return cmd + String.format("%s/bin/%s --config %s %s %s",
          getHDFSHome(), getHadoopVersion().startsWith("1") ? "hadoop" : "hdfs",
          getConfig(), service, op != null ? op : "");
    }
  }


  public HBaseClusterManager() {
    super();
  }

  protected CommandProvider getCommandProvider(final ServiceType service) {

    switch (service) {
      case HBASE_MASTER:
        return new HBaseShellCommandProvider();
      case HBASE_REGIONSERVER:
        return new HBaseShellCommandProvider();
      case ZOOKEEPER:
        return new HBaseShellCommandProvider();
      case HADOOP_DATANODE:
        return new HadoopShellCommandProvider();
      case HADOOP_NAMENODE:
        return new HadoopShellCommandProvider();
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Execute the given command on the host using SSH
   *
   * @return pair of exit code and command output
   * @throws IOException if something goes wrong.
   */
  private Pair<Integer, String> exec(String hostname, String... cmd) throws IOException {
    LOG.info("Executing remote command: " + StringUtils.join(cmd, " ") + " , hostname:" + hostname);

    RemoteShell shell = new RemoteShell(hostname, cmd);
    shell.execute();

    LOG.info("Executed remote command, exit code:" + shell.getExitCode()
        + " , output:" + shell.getOutput());

    return new Pair<Integer, String>(shell.getExitCode(), shell.getOutput());
  }

  private void execAsync(final String hostname, final String... cmd) throws IOException {
    Thread t = new Thread() {
      public void run() {
        try {
          exec(hostname, cmd);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();
  }

  private void exec(String hostname, ServiceType service, Operation op) throws IOException {
    if (service.getName().equals("namenode") || service.getName().equals("datanode")) {
      execAsync(hostname, getCommandProvider(service).getCommand(service, op));
    } else {
      exec(hostname, getCommandProvider(service).getCommand(service, op));
    }
  }

  @Override
  public void start(ServiceType service, String hostname) throws IOException {
    exec(hostname, service, Operation.START);
  }

  @Override
  public void stop(ServiceType service, String hostname) throws IOException {
    exec(hostname, service, Operation.STOP);
  }

  @Override
  public void restart(ServiceType service, String hostname) throws IOException {
    exec(hostname, service, Operation.RESTART);
  }

  @Override
  public void signal(ServiceType service, String signal, String hostname) throws IOException {
    exec(hostname, getCommandProvider(service).signalCommand(service, signal));
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname) throws IOException {
    String ret = exec(hostname, getCommandProvider(service).isRunningCommand(service))
        .getSecond();
    return ret.length() > 0;
  }


  public void formatNN(String hostname) throws IOException {
    exec(hostname,
        new HadoopShellCommandProvider().getCommand(ServiceType.HADOOP_NAMENODE, "-format"));
  }


  private File getDevSupportCmd(String prg) {
    File case1 = new File("." + File.separator + "dev-support" + File.separator + prg);
    File case2 = new File(".." + File.separator + "dev-support" + File.separator + prg);
    File toCheck = null;
    if (case1.exists()) {
      toCheck = case1;
    } else if (case2.exists()) {
      toCheck = case2;
    }
    Assert.assertNotNull(
        "Can't find " + case1.getAbsolutePath() + " or " + case2.getAbsolutePath(), toCheck);
    Assert.assertTrue(toCheck.canRead());
    Assert.assertTrue(toCheck.isFile());
    Assert.assertTrue(toCheck.canExecute());

    return toCheck;
  }

  // Kept in this class because it depends on HBase deployment by getDevSupportCmd
  @Override
  public void unplug(String hostname) throws Exception {
    File exec = getDevSupportCmd("it_tests_blockmachine_wrapper");
    String toExec = exec.getAbsolutePath() + " " + hostname;
    execLocally(toExec, exec.getParentFile());
  }

  @Override
  public void replug(String hostname) throws IOException, InterruptedException {
    File exec = getDevSupportCmd("it_tests_unblockmachine_wrapper");
    String toExec = exec.getAbsolutePath() + " " + hostname;
    execLocally(toExec, exec.getParentFile());
  }

  private void execLocally(String toExec, File path) throws InterruptedException, IOException {
    LOG.info("Executing locally " + toExec + "; in " + path.getAbsolutePath());
    Process p = Runtime.getRuntime().exec(toExec, null, path);
    p.waitFor();
    if (p.exitValue() != 0) {
      throw new IOException("Error executing " + toExec + " result is " + p.exitValue());
    }
  }

  /**
   * Kills all the java processus running on a computer
   */
  // Not in ClusterManager because it uses 'exec'
  public void killAllServices(String hostname) throws IOException {
    try {
      exec(hostname, "ps -ef | grep java | grep Dproc_ | cut -c 10-15 | xargs kill -9 ");
    } catch (Throwable ignored) {
      // it fails when there is no java process to kill - we ignore it
    }
  }


  public void rmDataDir(String hostname) throws IOException {
    try {
      exec(hostname, "rm -rf /tmp/*");
    } catch (Throwable ignored) {
      // it fails when we cannot delete everything. Ignoring
    }
  }

  /**
   * Check that we can access to a remove server by pinging it.
   *
   * @throws IOException          if it can"t connect
   * @throws InterruptedException if interrupted during the wait for the ping
   */
  public void checkAccessible(String hostname) throws IOException, InterruptedException {
    execLocally("ping -c 1 " + hostname, new File("."));
  }
}
