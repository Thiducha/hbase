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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.master.metrics.MasterMetricsSource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 *  Factory for classes supplied by hadoop compatibility modules.
 */
public class CompatibilitySingletonFactory {
  private static final Log LOG = LogFactory.getLog(CompatibilitySingletonFactory.class);
  public static final String EXCEPTION_START = "Could not create  ";
  public static final String EXCEPTION_END = " Is the hadoop compatibility jar on the classpath?";

  private static final Map<Class, Object> instances = new HashMap<Class, Object>();

  /**
   * Get the singleton instance of Any classes defined by compatibiliy jar's
   *
   * @return the singleton
   */
  public static synchronized <T> T getInstance(Class<T> klass) {
    T instance = (T) instances.get(klass);
    if (instance == null) {
      try {
        ServiceLoader<T> loader = ServiceLoader.load(klass);
        Iterator<T> it = loader.iterator();
        instance = it.next();
        if (it.hasNext()) {
          StringBuilder msg = new StringBuilder();
          msg.append("ServiceLoader provided more than one implementation for class: ")
                  .append(klass)
                  .append(", using implementation: ").append(instance.getClass())
                  .append(", other implementations: {");
          while (it.hasNext()) {
            msg.append(it.next()).append(" ");
          }
          msg.append("}");
          LOG.warn(msg);
        }
      } catch (Exception e) {
        throw new RuntimeException(createExceptionString(klass), e);
      } catch (Error e) {
        throw new RuntimeException(createExceptionString(klass), e);
      }

      // If there was nothing returned and no exception then throw an exception.
      if (instance == null) {
        throw new RuntimeException(createExceptionString(klass));
      }
      instances.put(klass, instance);
    }
    return instance;
  }

  private static String createExceptionString(Class klass) {
     return EXCEPTION_START + klass.toString() + EXCEPTION_END;
  }

}
