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

package co.cask.wrangler.expression;

import co.cask.cdap.api.common.Bytes;
import co.cask.functions.DDL;
import co.cask.functions.DataQuality;
import co.cask.functions.Dates;
import co.cask.functions.GeoFences;
import co.cask.functions.Global;
import co.cask.functions.JSON;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class <code>EL</code> is a Expression Language Handler.
 */
public final class EL {
  private Set<String> variables = new HashSet<>();
  private final JexlEngine engine;
  private JexlScript script = null;

  public EL(ELRegistration registration) {
    engine = new JexlBuilder()
      .namespaces(registration.functions())
      .silent(false)
      .cache(1024)
      .strict(true)
      .logger(new NullLogger())
      .create();
  }

  public void compile(String expression) throws ELException {
    variables.clear();
    try {
      script = engine.createScript(expression);
      Set<List<String>> varSet = script.getVariables();
      for (List<String> vars : varSet) {
        variables.add(Joiner.on(".").join(vars));
      }
    } catch (JexlException e) {
      if (e.getCause() != null) {
        throw new ELException(e.getCause().getMessage());
      } else {
        throw new ELException(e.getMessage());
      }
    } catch (Exception e) {
      throw new ELException(e.getMessage());
    }
  }

  public Set<String> variables() {
    return variables;
  }

  public ELResult execute(ELContext context, boolean nullMissingFields) throws ELException {
    try {
      if (nullMissingFields) {
        for (String variable : variables) {
          if (!context.has(variable)) {
            context.add(variable, null);
          }
        }
      }
      Object value = script.execute(context);
      ELResult variable = new ELResult(value);
      return variable;
    } catch (JexlException e) {
      // Generally JexlException wraps the original exception, so it's good idea
      // to check if there is a inner exception, if there is wrap it in 'DirectiveExecutionException'
      // else just print the error message.
      if (e.getCause() != null) {
        throw new ELException(e.getCause().getMessage());
      } else {
        throw new ELException(e.getMessage());
      }
    } catch (NumberFormatException e) {
      throw new ELException("Type mismatch. Change type of constant " +
                              "or convert to right data type using conversion functions available. Reason : "
                              + e.getMessage());
    } catch (Exception e) {
      if (e.getCause() != null) {
        throw new ELException(e.getCause().getMessage());
      } else {
        throw new ELException(e.getMessage());
      }
    }
  }

  public ELResult execute(ELContext context) throws ELException {
    return execute(context, true);
  }

  /**
   * @return List of registered functions.
   */
  public static final class DefaultFunctions implements ELRegistration {
    @Override
    public Map<String, Object> functions() {
      Map<String, Object> functions = new HashMap<>();
      functions.put(null, Global.class);
      functions.put("date", Dates.class);
      functions.put("json", JSON.class);
      functions.put("math", Math.class);
      functions.put("string", StringUtils.class);
      functions.put("strings", Strings.class);
      functions.put("escape", StringEscapeUtils.class);
      functions.put("bytes", Bytes.class);
      functions.put("arrays", Arrays.class);
      functions.put("dq", DataQuality.class);
      functions.put("ddl", DDL.class);
      functions.put("geo", GeoFences.class);
      return functions;
    }

  }

  private final class NullLogger implements Log {
    @Override
    public void debug(Object o) {

    }

    @Override
    public void debug(Object o, Throwable throwable) {

    }

    @Override
    public void error(Object o) {

    }

    @Override
    public void error(Object o, Throwable throwable) {

    }

    @Override
    public void fatal(Object o) {

    }

    @Override
    public void fatal(Object o, Throwable throwable) {

    }

    @Override
    public void info(Object o) {

    }

    @Override
    public void info(Object o, Throwable throwable) {

    }

    @Override
    public boolean isDebugEnabled() {
      return false;
    }

    @Override
    public boolean isErrorEnabled() {
      return false;
    }

    @Override
    public boolean isFatalEnabled() {
      return false;
    }

    @Override
    public boolean isInfoEnabled() {
      return false;
    }

    @Override
    public boolean isTraceEnabled() {
      return false;
    }

    @Override
    public boolean isWarnEnabled() {
      return false;
    }

    @Override
    public void trace(Object o) {

    }

    @Override
    public void trace(Object o, Throwable throwable) {

    }

    @Override
    public void warn(Object o) {

    }

    @Override
    public void warn(Object o, Throwable throwable) {

    }
  }
}
