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

package co.cask.directives.writer;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

/**
 * A step to write the record fields as CSV.
 */
@Plugin(type = Directive.TYPE)
@Name("write-as-csv")
@Categories(categories = { "writer", "csv"})
@Description("Writes the records files as well-formatted CSV")
public class WriteAsCSV implements Directive {
  public static final String NAME = "write-as-csv";
  private String column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      try {
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        try (Writer out = new BufferedWriter(new OutputStreamWriter(bOut))) {
          CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.DEFAULT);

          for (int i = 0; i < row.length(); ++i) {
            csvPrinter.print(row.getValue(i));
          }
          csvPrinter.flush();
          csvPrinter.close();
        } catch (Exception e) {
          bOut.close();
        }
        row.add(column, bOut.toString());
      } catch (IOException e) {
        throw new DirectiveExecutionException(toString() + " : Failed to write CSV row. " + e.getMessage());
      }
    }
    return rows;
  }
}
