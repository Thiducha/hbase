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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
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

  /**
   * CommandProvider to manage the service using bin/hbase-* scripts
   */
  static class HBaseShellCommandProvider extends CommandProvider {
    private String getHBaseHome() {
      return "/home/liochon/tmp-recotest/hbase";
    }

    private String getConfig() {
      return "/home/liochon/tmp-recotest/hbase/conf";
    }

    @Override
    public String getCommand(ServiceType service, Operation op) {
      String cmd = "";
      cmd += "JAVA_HOME=/opt/jdk1.6";
      cmd += "export HADOOP_SSH_OPTS='-A'";
      cmd += "export HBASE_HEAPSIZE=500;";
      cmd += "export HBASE_CONF_DIR=" + getConfig() + ";";
      cmd += "export HBASE_HOME=" + getHBaseHome() + ";";
      return cmd + String.format("%s/bin/hbase-daemon%s.sh %s  %s",
          service.equals(ServiceType.ZOOKEEPER) ? "s" : "",
          getHBaseHome(),
          op.toString().toLowerCase(), service);
    }

    public String getDevSupportCommand(String cmd, String param1) {
      return String.format("%s/dev-support/%s %s", getHBaseHome(), cmd, param1);
    }
  }


  /**
   * CommandProvider to manage the service using bin/hbase-* scripts
   */
  static class HadoopShellCommandProvider extends CommandProvider {
    private String getConfig() {
      return "/home/liochon/tmp-recotest/conf/conf-dn/";
    }

    @Override
    public String getCommand(ServiceType service, Operation op) {
      String hadoopCommonHome = "/home/liochon/tmp-recotest/hadoop-common/build/hadoop-1.1-NICO_2";


      String cmd = "";
      cmd += "JAVA_HOME=/opt/jdk1.6";
      cmd = "export HADOOP_COMMON_HOME=" + hadoopCommonHome + ";";
      cmd += "export HADOOP_HDFS_HOME=/home/liochon/tmp-recotest/hadoop-common/build/hadoop-1.1-NICO_2;";

      // can only start hadoop commands
      return cmd + String.format("%s/bin/hadoop --config %s %s", hadoopCommonHome, getConfig(), service);
    }

    public String getDevSupportCommand(String cmd, String param1) {
      return "";
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

  private void exec(String hostname, ServiceType service, Operation op) throws IOException {
    exec(hostname, getCommandProvider(service).getCommand(service, op));
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

  public void checkAccessible(String hostname) throws Exception {
    Process p = Runtime.getRuntime().exec("ping -c 1 " + hostname);
    p.waitFor();
    if (p.exitValue() != 0) {
      throw new Exception("Can't ping " + hostname + " I can't access it");
    }
  }


  @Override
  public void unplug(String hostname) throws IOException {
    exec("127.0.0.1", new HBaseShellCommandProvider().
        getDevSupportCommand("it_tests_unblockmachine_wrapper", hostname));
  }

  @Override
  public void replug(String hostname) throws IOException {
    exec("127.0.0.1", new HBaseShellCommandProvider().
        getDevSupportCommand("it_tests_blockmachine_wrapper", hostname));
  }

}
