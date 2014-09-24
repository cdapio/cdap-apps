/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * This is a base Flowlet for implementing Flowlet that do actual processing with external local process.
 *
 * @param <IN> input type for process method.
 * @param <OUT> output type for process method.
 */
public abstract class ExternalProgramFlowlet<IN, OUT> extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalProgramFlowlet.class);

  private List<ExternalProgramExecutor> executors;
  private Iterator<ExternalProgramExecutor> executorIterator;
  private OutputHelper<OUT> outputHelper;

  @Override
  public final void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    executors = Lists.newArrayList(init(context).createExecutors(context.getName()));
    executorIterator = Iterators.cycle(executors);

    outputHelper = new OutputHelper<OUT>(getOutputEmitter());

    List<ListenableFuture<Service.State>> startFutures = Lists.newArrayListWithCapacity(executors.size());
    for (ExternalProgramExecutor executor : executors) {
      startFutures.add(executor.start());
    }

    try {
      Futures.allAsList(startFutures).get();
    } catch (Exception e) {
      LOG.error("Failed to start external program.", e);
      stopAndWait(executors);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public final void destroy() {
    finish();
    try {
      stopAndWait(executors);
    } catch (Exception e) {
      LOG.error("Failed to shutdown external program.", e);
    }
  }

  /**
   * This is the flowlet process method that takes input of type {@code IN}.
   * Children class can override this method to use different batch size and assign name to the input, and
   * simply calling {@link #process(java.util.Iterator) super.process(iterator)} to perform the actual processing.
   *
   * @param iterator Iterator to gives events to process.
   */
  @Batch(100)
  @ProcessInput
  protected void process(Iterator<IN> iterator) throws Exception {
    List<ListenableFuture<String>> completions = Lists.newLinkedList();

    try {
      while (iterator.hasNext()) {
        IN input = iterator.next();
        String encoded = encode(input);
        if (encoded == null) {
          continue;
        }

        SettableFuture<String> completion = SettableFuture.create();
        executorIterator.next().submit(encoded, completion);
        completions.add(completion);
      }

      // Wait for completion of all requests
      Futures.successfulAsList(completions).get();

      // Process and output
      for (ListenableFuture<String> completion : completions) {
        OUT output = processResult(completion.get());
        outputHelper.emit(output);
      }
    } finally {
      // Check program status. If any of them died, restart it.
      ListIterator<ExternalProgramExecutor> listIterator = executors.listIterator();
      while (listIterator.hasNext()) {
        ExternalProgramExecutor executor = listIterator.next();
        if (!executor.isRunning()) {
          LOG.warn("Program {} terminated. Starting a new one.", executor);
          ExternalProgramExecutor replacement = new ExternalProgramExecutor(executor);
          replacement.startAndWait();
          listIterator.set(replacement);
        }
      }
    }
  }

  /**
   * This method will be called at Flowlet initialization time.
   *
   * @param context The {@link FlowletContext} for this Flowlet.
   * @return An {@link ExternalProgram} to specify properties of the external program to process input.
   */
  protected abstract ExternalProgram init(FlowletContext context);

  /**
   * This method will be called for each input event to transform the given input into string before sending to
   * external program for processing.
   *
   * @param input The input event.
   * @return A UTF-8 encoded string of the input, or {@code null} if to skip this input.
   */
  protected abstract String encode(IN input);

  /**
   * This method will be called when the external program returns the result. Child class can do it's own processing
   * in this method or could return an object of type {@code OUT} for emitting to next flowlet with the
   * {@link OutputEmitter} returned by {@link #getOutputEmitter()}.
   *
   * @param result The result from the external program.
   * @return The output to emit or {@code null} if nothing to emit.
   */
  protected abstract OUT processResult(String result);

  /**
   * Child class can override this method to return an OutputEmitter for writing data to the next flowlet.
   *
   * @return An {@link OutputEmitter} for type {@code OUT}, or {@code null} if this flowlet doesn't have output.
   */
  protected OutputEmitter<OUT> getOutputEmitter() {
    return null;
  }

  /**
   * This method will be called when this Flowlet shutdown.
   */
  protected void finish() {
    // No-op
  }

  private void stopAndWait(Iterable<ExternalProgramExecutor> executors) throws Exception {
    Uninterruptibles.getUninterruptibly(Futures.successfulAsList(
      Iterables.transform(executors, new Function<ExternalProgramExecutor, ListenableFuture<?>>() {
        @Override
        public ListenableFuture<?> apply(ExternalProgramExecutor executor) {
          return executor.stop();
        }
      })));
  }

  /**
   * Represents an external program used by this flowlet.
   */
  protected static final class ExternalProgram {
    private final File executable;
    private final int instances;
    private final String[] args;

    /**
     * Constructs an ExternalProgram with one instance.
     * @param executable The file path to the executable in the local system.
     * @param args Array of arguments for the program.
     */
    public ExternalProgram(File executable, String...args) {
      this(1, executable, args);
    }

    /**
     * Constructs an ExternalProgram with the specified number of instances.
     * @param executable The file path to the executable in the local system.
     * @param args Array of arguments for the program.
     * @param instances Number of instances of the program to use for this flowlet.
     */
    public ExternalProgram(int instances, File executable, String...args) {
      Preconditions.checkArgument(instances > 0, "Instance count should be > 0.");
      this.instances = instances;
      this.executable = executable;
      this.args = Arrays.copyOf(args, args.length);
    }

    List<ExternalProgramExecutor> createExecutors(String name) {
      ImmutableList.Builder<ExternalProgramExecutor> executors = ImmutableList.builder();
      for (int i = 0; i < instances; i++) {
        executors.add(new ExternalProgramExecutor(name + "-" + i, executable, args));
      }

      return executors.build();
    }
  }

  private static final class OutputHelper<OUT> {
    private final OutputEmitter<OUT> emitter;

    private OutputHelper(OutputEmitter<OUT> emitter) {
      this.emitter = emitter;
    }

    void emit(OUT output) {
      if (emitter != null && output != null) {
        emitter.emit(output);
      }
    }
  }
}
