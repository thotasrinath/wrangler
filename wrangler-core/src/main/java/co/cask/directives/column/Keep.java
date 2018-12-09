/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.column;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnNameList;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class <code>Keep</code> implements a directive that
 * opposite of {@link Drop} columns. Instead of dropping the
 * columns specified, it keeps only those columns that are
 * specified.
 */
@Plugin(type = Directive.TYPE)
@Name("keep")
@Categories(categories = { "column"})
@Description("Keeps the specified columns and drops all others.")
public class Keep implements Directive {
  public static final String NAME = "keep";
  private final Set<String> keep = new HashSet<>();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    ColumnNameList cols = args.value("column");
    for (String col : cols.value()) {
      keep.add(col);
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = 0;
      for (Pair<String, Object> v : row.getFields()) {
        if (!keep.contains(v.getFirst())) {
          row.remove(idx);
        } else {
          ++idx;
        }
      }
    }
    return rows;
  }
}
