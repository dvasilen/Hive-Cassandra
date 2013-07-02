/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra;

import org.apache.cassandra.service.CassandraDaemon;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class EmbeddedServer {
  protected static CassandraDaemon daemon = null;

  static ExecutorService executor = Executors.newSingleThreadExecutor();

  public static void startCas() {
    executor.execute(new Runnable() {
      public void run() {
        daemon = new CassandraDaemon();
        daemon.activate();
      }
    });
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  public static void stopBrisk() throws Exception {
    if (daemon != null) {
      daemon.deactivate();
    }
    executor.shutdown();
    executor.shutdownNow();
  }

}
