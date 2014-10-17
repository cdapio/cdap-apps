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

package co.cask.cdap.apps.sentiment;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.apps.flowlet.ExternalProgramFlowlet;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Analyzes the sentences by passing the sentence to NLTK based sentiment analyzer
 * written in Python.
 */
public class PythonAnalyzer extends ExternalProgramFlowlet<Tweet, Tweet> {
  private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);
  private static final Gson GSON = new Gson();

  private OutputEmitter<Tweet> sentiment;

  private File workDir;

  /**
   * This method will be called at Flowlet initialization time.
   *
   * @param context The {@link co.cask.cdap.api.flow.flowlet.FlowletContext} for this Flowlet.
   * @return An {@link co.cask.cdap.apps.flowlet.ExternalProgramFlowlet.ExternalProgram} to specify
   * properties of the external program to process input.
   */
  @Override
  protected ExternalProgram init(FlowletContext context) {
    try {
      InputStream in = this.getClass().getClassLoader().getResourceAsStream("sentiment-process.zip");

      if (in != null) {
        workDir = new File("work");
        Unzipper.unzip(in, workDir);

        File python = new File("/usr/bin/python");
        File program = new File(workDir, "sentiment/score_sentiment.py");

        if (python.exists()) {
          return new ExternalProgram(python, program.getAbsolutePath());
        }
      }

      throw new RuntimeException("Unable to start process");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Batch(100)
  @ProcessInput("python")
  public void process(Iterator<Tweet> iterator) throws Exception {
    super.process(iterator);
  }

  /**
   * This method will be called for each input event to transform the given input into string before sending to
   * external program for processing.
   *
   * @param input The input event.
   * @return A UTF-8 encoded string of the input, or {@code null} if to skip this input.
   */
  @Override
  protected String encode(Tweet input) {
    return GSON.toJson(input);
  }

  /**
   * This method will be called when the external program returns the result. Child class can do its own processing
   * in this method or could return an object of type {@code OUT} for emitting to next Flowlet with the
   * {@link co.cask.cdap.api.flow.flowlet.OutputEmitter} returned by {@link #getOutputEmitter()}.
   *
   * @param result The result from the external program.
   * @return The output to emit or {@code null} if nothing to emit.
   */
  @Override
  protected Tweet processResult(String result) {
    return GSON.fromJson(result, Tweet.class);
  }

  /**
   * Child class can override this method to return an OutputEmitter for writing data to the next Flowlet.
   *
   * @return An {@link co.cask.cdap.api.flow.flowlet.OutputEmitter} for type {@code OUT}, or {@code null} if
   * this flowlet doesn't have output.
   */
  @Override
  protected OutputEmitter<Tweet> getOutputEmitter() {
    return sentiment;
  }

  @Override
  protected void finish() {
    if (workDir == null) {
      return;
    }
    try {
      LOG.info("Deleting work dir {}", workDir);
      FileUtils.deleteDirectory(workDir);
    } catch (IOException e) {
      LOG.error("Could not delete work dir {}", workDir);
      throw Throwables.propagate(e);
    }
  }
}