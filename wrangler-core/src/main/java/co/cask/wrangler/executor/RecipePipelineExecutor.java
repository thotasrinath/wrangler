/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.executor;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveNotFoundException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.RecipeException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.ReportErrorAndProceed;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.TransientVariableScope;
import co.cask.wrangler.utils.RecordConvertor;
import co.cask.wrangler.utils.RecordConvertorException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The class <code>RecipePipelineExecutor</code> compiles the recipe and executes
 * the directives.
 */
public final class RecipePipelineExecutor implements RecipePipeline<Row, StructuredRecord, ErrorRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RecipePipelineExecutor.class);
  private ExecutorContext context;
  private List<Executor> directives;
  private final ErrorRecordCollector collector = new ErrorRecordCollector();
  private RecordConvertor convertor = new RecordConvertor();

  /**
   * Configures the pipeline based on the directives. It parses the recipe,
   * converting it into executable directives.
   *
   * @param parser Wrangle directives parser.
   */
  @Override
  public void initialize(RecipeParser parser, ExecutorContext context) throws RecipeException {
    this.context = context;
    try {
      this.directives = parser.parse();
    } catch (DirectiveParseException e) {
      throw new RecipeException(e.getMessage());
    } catch (DirectiveNotFoundException | DirectiveLoadException e) {
      throw new RecipeException(e.getMessage(), e);
    }
  }

  /**
   * Invokes each directives destroy method to perform any cleanup
   * required by each individual directive.
   */
  @Override
  public void destroy() {
    for (Executor directive : directives) {
      try {
        directive.destroy();
      } catch (Throwable t) {
        LOG.warn(t.getMessage());
      }
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param rows List of Input record of type I.
   * @param schema Schema to which the output should be mapped.
   * @return Parsed output list of record of type O
   */
  @Override
  public List<StructuredRecord> execute(List<Row> rows, Schema schema)
    throws RecipeException {
    rows = execute(rows);
    try {
      List<StructuredRecord> output = convertor.toStructureRecord(rows, schema);
      return output;
    } catch (RecordConvertorException e) {
      throw new RecipeException("Problem converting into output record. Reason : " + e.getMessage());
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param rows List of input record of type I.
   * @return Parsed output list of record of type I
   */
  @Override
  public List<Row> execute(List<Row> rows) throws RecipeException {
    List<String> messages = new ArrayList<>();
    List<Row> results = Lists.newArrayList();
    try {
      int i = 0;
      collector.reset();
      while (i < rows.size()) {
        messages.clear();
        // Resets the scope of local variable.
        if (context != null) {
          context.getTransientStore().reset(TransientVariableScope.LOCAL);
        }
        List<Row> newRows = rows.subList(i, i + 1);
        try {
          for (Executor<List<Row>, List<Row>> directive : directives) {
            try {
              newRows = directive.execute(newRows, context);
              if (newRows.size() < 1) {
                break;
              }
            } catch (ReportErrorAndProceed e) {
              messages.add(String.format("%d:%s", e.getCode(), e.getMessage()));
            }
          }
          if (newRows.size() > 0) {
            results.addAll(newRows);
          }
        } catch (ErrorRowException e) {
          messages.add(String.format("%s", e.getMessage()));
          collector.add(new ErrorRecord(newRows.get(0), String.join(",", messages), e.getCode()));
        }
        i++;
      }
    } catch (DirectiveExecutionException e) {
      throw new RecipeException(e.getMessage(), e);
    }
    return results;
  }

  /**
   * Returns records that are errored out.
   *
   * @return records that have errored out.
   */
  @Override
  public List<ErrorRecord> errors() {
    return collector.get();
  }
}
