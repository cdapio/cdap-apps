/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.apps.flowlet;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A package private class for executing an external program.
 */
final class ExternalProgramExecutor extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalProgramExecutor.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final String name;
  private final File executable;
  private final String[] args;
  private final BlockingQueue<Event> eventQueue;
  private ExecutorService executor;
  private Process process;
  private Thread shutdownThread;

  ExternalProgramExecutor(String name, File executable, String...args) {
    this.name = name;
    this.executable = executable;
    this.args = args;
    this.eventQueue = new LinkedBlockingQueue<Event>();
  }

  /**
   * Creates a new {@link ExternalProgramExecutor} using settings from another one.
   * @param other {@link ExternalProgramExecutor} to clone from.
   */
  ExternalProgramExecutor(ExternalProgramExecutor other) {
    this.name = other.name;
    this.executable = other.executable;
    this.args = other.args;
    this.eventQueue = new LinkedBlockingQueue<Event>();
  }

  void submit(String line, SettableFuture<String> completion) {
    Preconditions.checkState(isRunning(), "External program {} is not running.", this);
    try {
      eventQueue.put(new Event(line, completion));
    } catch (InterruptedException e) {
      completion.setException(e);
    }
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", name, executable.getName(), Arrays.toString(args));
  }

  @Override
  protected String getServiceName() {
    return super.getServiceName() + "-" + name;
  }

  @Override
  protected void startUp() throws Exception {
    // We need two threads.
    // One thread for keep reading from input, write to process stdout and read from stdin.
    // The other for keep reading stderr and log.
    executor = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("process-" + name + "-%d").build());

    // the Shutdown thread is to time the shutdown and kill the process if it timeout.
    shutdownThread = createShutdownThread();

    List<String> cmd = ImmutableList.<String>builder().add(executable.getAbsolutePath()).add(args).build();
    process = new ProcessBuilder(cmd).start();

    executor.execute(createProcessRunnable(process));
    executor.execute(createLogRunnable(process));
  }

  @Override
  protected void run() throws Exception {
    // Simply wait for the process to complete.
    // Trigger shutdown would trigger closing of in/out streams of the process,
    // which if the process implemented correctly, should complete.
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      LOG.error("Process {} exit with exit code {}", this, exitCode);
    }
  }

  @Override
  protected void triggerShutdown() {
    executor.shutdownNow();
    shutdownThread.start();
  }

  @Override
  protected void shutDown() throws Exception {
    shutdownThread.interrupt();
    shutdownThread.join();

    // Need to notify all pending events as failure.
    List<Event> events = Lists.newLinkedList();
    eventQueue.drainTo(events);

    for (Event event : events) {
      event.getCompletion().setException(
        new IllegalStateException("External program " + toString() + " already stopped."));
    }
  }

  private Thread createShutdownThread() {
    Thread t = new Thread("shutdown-" + name) {
      @Override
      public void run() {
        // Wait for at most SHUTDOWN_TIME and kill the process
        try {
          TimeUnit.SECONDS.sleep(SHUTDOWN_TIMEOUT_SECONDS);
          LOG.warn("Process {} took too long to stop. Killing it.", this);
          process.destroy();
        } catch (InterruptedException e) {
          // If interrupted, meaning the process has been shutdown nicely.
        }
      }
    };

    t.setDaemon(true);
    return t;
  }

  private Runnable createProcessRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charsets.UTF_8));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(process.getOutputStream(), Charsets.UTF_8), true);

        LOG.info("Start process {}", ExternalProgramExecutor.this);

        try {
          while (!Thread.currentThread().isInterrupted()) {
            Event event = eventQueue.take();
            try {
              writer.println(event.getLine());
              event.getCompletion().set(reader.readLine());
            } catch (IOException e) {
              LOG.error("Exception when sending events to {}. Event: {}.",
                        ExternalProgramExecutor.this, event.getLine());
              break;
            }
          }
        } catch (InterruptedException e) {
          // No-op. It's just for signal stopping of the thread.
        } finally {
          Closeables.closeQuietly(writer);
          Closeables.closeQuietly(reader);
        }

        LOG.info("Process completed {}.", ExternalProgramExecutor.this);
      }
    };
  }

  private Runnable createLogRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), Charsets.UTF_8));
        try {
          String line = reader.readLine();
          while (!Thread.currentThread().isInterrupted() && line != null) {
            LOG.info(line);
            line = reader.readLine();
          }
        } catch (IOException e) {
          LOG.error("Exception when reading from stderr stream for {}.", ExternalProgramExecutor.this);
        } finally {
          Closeables.closeQuietly(reader);
        }
      }
    };
  }

  private static final class Event {
    private final String line;
    private final SettableFuture<String> completion;

    Event(String line, SettableFuture<String> completion) {
      this.line = line;
      this.completion = completion;
    }

    private String getLine() {
      return line;
    }

    private SettableFuture<String> getCompletion() {
      return completion;
    }
  }
}
