/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.service.dbs;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.directives.aggregates.DefaultTransientStore;
import co.cask.wrangler.api.*;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceException;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.ConfigDirectiveContext;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.parser.RecipeCompiler;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.registry.UserDirectiveRegistry;
import co.cask.wrangler.service.directive.RequestDeserializer;
import co.cask.wrangler.service.directive.ServicePipelineContext;
import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.joining.DataFrameJoiner;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.net.HttpURLConnection;
import java.util.*;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;

public class JoinSuggesterService extends AbstractHttpServiceHandler {

    public static final String WORKSPACE_DATASET = "workspace";

    private static final Gson GSON =
            new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

    @UseDataSet(WORKSPACE_DATASET)
    private WorkspaceDataset table;

    private DirectiveRegistry composite;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
        super.initialize(context);
        composite = new CompositeDirectiveRegistry(
                new SystemDirectiveRegistry(),
                new UserDirectiveRegistry(context)
        );
    }

    @GET
    @Path("joinsuggest/{workspaceId1}/{workspaceId2}")
    public void list(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("workspaceId1") String workspaceId1, @PathParam("workspaceId2") String workspaceId2) {
        try {


            Table t1 = getTableFromWorkspace(workspaceId1);
            Table t2 = getTableFromWorkspace(workspaceId2);

            JsonArray array = new JsonArray();

            for (Column columns1 : t1.columnArray()) {

                for (Column columns2 : t2.columnArray()) {

                    DataFrameJoiner dataFrameJoiner = new DataFrameJoiner(t1, columns1.name());
                    Table t3 = dataFrameJoiner.inner(t2, columns2.name());

                    int rowCount = t3.rowCount();

                    JsonObject object = new JsonObject();
                    object.addProperty("T1-Col", columns1.name());
                    object.addProperty("T2-Col", columns2.name());
                    object.addProperty("count", rowCount);
                    array.add(object);

                }

            }


            JsonObject response = new JsonObject();

            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success");
            response.add("value", array);

            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        } catch (Exception e) {
            error(responder, e.getMessage());
        }
    }


    private Table getTableFromWorkspace(String ws) throws Exception {


        Table tablesaw = Table.create(ws);

        Request user = getContent("UTF-8", Bytes.toString(table.getData(ws, WorkspaceDataset.REQUEST_COL)), Request.class);

        user.getRecipe().setPragma(addLoadablePragmaDirectives(user));


        List<Row> rows = executeDirectives(ws, user, new Function<List<Row>, List<Row>>() {
            @Nullable
            @Override
            public List<Row> apply(@Nullable List<Row> records) {

                return records;
            }
        });

        Map<String, List<String>> data = new HashMap<>();


        for (Row row : rows) {

            row.getFields().forEach(m -> {

                if (data.containsKey(m.getFirst())) {

                    List list = data.get(m.getFirst());

                    list.add(m.getSecond());

                } else {

                    List list = new ArrayList();
                    list.add(m.getSecond());

                    data.put(m.getFirst(), list);

                }

            });
        }

        for (Map.Entry<String, List<String>> entry : data.entrySet()) {

            String[] arr = new String[entry.getValue().size()];

            int i=0;
            for (String s : entry.getValue()){
                arr[i]=s;
                i++;
            }

            tablesaw = tablesaw.addColumns(StringColumn.create(entry.getKey(), arr));

        }

        return tablesaw;

    }

    /**
     * Converts the data in workspace into records.
     *
     * @param id name of the workspace from which the records are generated.
     * @return list of records.
     * @throws WorkspaceException thrown when there is issue retrieving data.
     */
    private List<Row> fromWorkspace(String id) throws WorkspaceException {
        DataType type = table.getType(id);
        List<Row> rows = new ArrayList<>();

        if (type == null) {
            throw new WorkspaceException("Workspace you are currently working on seemed to have " +
                    "disappeared, please reload the data.");
        }

        switch (type) {
            case TEXT: {
                String data = table.getData(id, WorkspaceDataset.DATA_COL, DataType.TEXT);
                if (data != null) {
                    rows.add(new Row("body", data));
                }
                break;
            }

            case BINARY: {
                byte[] data = table.getData(id, WorkspaceDataset.DATA_COL, DataType.BINARY);
                if (data != null) {
                    rows.add(new Row("body", data));
                }
                break;
            }

            case RECORDS: {
                rows = table.getData(id, WorkspaceDataset.DATA_COL, DataType.RECORDS);
                break;
            }
        }
        return rows;
    }

    /**
     * Executes directives by extracting them from request.
     *
     * @param id     data to be used for executing directives.
     * @param user   request passed on http.
     * @param sample sampling function.
     * @return records generated from the directives.
     */
    private List<Row> executeDirectives(String id, @Nullable Request user,
                                        Function<List<Row>, List<Row>> sample)
            throws Exception {
        if (user == null) {
            throw new Exception("Request is empty. Please check if the request is sent as HTTP POST body.");
        }

        TransientStore store = new DefaultTransientStore();
        // Extract rows from the workspace.
        List<Row> rows = fromWorkspace(id);
        // Execute the pipeline.
        ExecutorContext context = new ServicePipelineContext(ExecutorContext.Environment.SERVICE,
                getContext(),
                store);
        RecipePipelineExecutor executor = new RecipePipelineExecutor();
        if (user.getRecipe().getDirectives().size() > 0) {
            GrammarMigrator migrator = new MigrateToV2(user.getRecipe().getDirectives());
            String migrate = migrator.migrate();
            RecipeParser recipe = new GrammarBasedParser(migrate, composite);
            recipe.initialize(new ConfigDirectiveContext(table.getConfigString()));
            executor.initialize(recipe, context);
            rows = executor.execute(sample.apply(rows));
            executor.destroy();
        }
        return rows;
    }

    /**
     * Automatically adds a load-directives pragma to the list of directives.
     */
    private String addLoadablePragmaDirectives(Request request) {
        StringBuilder sb = new StringBuilder();
        // Validate the DSL by compiling the DSL. In case of macros being
        // specified, the compilation will them at this phase.
        Compiler compiler = new RecipeCompiler();
        try {
            // Compile the directive extracting the loadable plugins (a.k.a
            // Directives in this context).
            CompileStatus status = compiler.compile(new MigrateToV2(request.getRecipe().getDirectives()).migrate());
            RecipeSymbol symbols = status.getSymbols();
            Iterator<TokenGroup> iterator = symbols.iterator();
            List<String> userDirectives = new ArrayList<>();
            while (iterator.hasNext()) {
                TokenGroup next = iterator.next();
                if (next == null || next.size() < 1) {
                    continue;
                }
                Token token = next.get(0);
                if (token.type() == TokenType.DIRECTIVE_NAME) {
                    String directive = (String) token.value();
                    try {
                        DirectiveInfo.Scope scope = composite.get(directive).scope();
                        if (scope == DirectiveInfo.Scope.USER) {
                            userDirectives.add(directive);
                        }
                    } catch (DirectiveLoadException e) {
                        // no-op.
                    }
                }
            }
            if (userDirectives.size() > 0) {
                sb.append("#pragma load-directives ");
                String directives = StringUtils.join(userDirectives, ",");
                sb.append(directives).append(";");
                return sb.toString();
            }
        } catch (CompileException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        } catch (DirectiveParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return null;
    }

    public <T> T getContent(String charset, String data, Class<?> type) {

        if (data != null) {
            GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter(Request.class, new RequestDeserializer());
            Gson gson = builder.create();
            return (T) gson.fromJson(data, type);
        }
        return null;
    }
}


