/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertRelDataTypeToExprType;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.opensearch.sql.ast.statement.Explain.ExplainFormat;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.util.JdbcOpenSearchDataTypeConvertor;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch execution engine implementation. */
@RequiredArgsConstructor
public class OpenSearchExecutionEngine implements ExecutionEngine {

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;
  private final PlanSerializer planSerializer;

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    execute(physicalPlan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan physicalPlan,
      ExecutionContext context,
      ResponseListener<QueryResponse> listener) {
      System.out.println("OpenSearchExecutionEngine.execute");
      System.out.println("OpenSearchExecutionEngine.execute" + physicalPlan);
      PhysicalPlan plan = executionProtector.protect(physicalPlan);
      System.out.println("OpenSearchExecutionEngine.execute" + plan);
    client.schedule(
        () -> {
          try {
            List<ExprValue> result = new ArrayList<>();

            context.getSplit().ifPresent(plan::add);
            plan.open();

            while (plan.hasNext()) {
              result.add(plan.next());
            }

            QueryResponse response =
                new QueryResponse(
                    physicalPlan.schema(), result, planSerializer.convertToCursor(plan));
            listener.onResponse(response);
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            Explain openSearchExplain =
                new Explain() {
                  @Override
                  public ExplainResponseNode visitTableScan(
                      TableScanOperator node, Object context) {
                    return explain(
                        node,
                        context,
                        explainNode -> {
                          explainNode.setDescription(Map.of("request", node.explain()));
                        });
                  }
                };

            listener.onResponse(openSearchExplain.apply(plan));
          } catch (Exception e) {
            listener.onFailure(e);
          }
        });
  }

  private Hook.Closeable getPhysicalPlanInHook(
      AtomicReference<String> physical, SqlExplainLevel level) {
    return Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
        obj -> {
          RelRoot relRoot = (RelRoot) obj;
          physical.set(RelOptUtil.toString(relRoot.rel, level));
        });
  }

  private Hook.Closeable getCodegenInHook(AtomicReference<String> codegen) {
    return Hook.JAVA_PLAN.addThread(
        obj -> {
          codegen.set((String) obj);
        });
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainFormat format,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            if (format == ExplainFormat.SIMPLE) {
              String logical = RelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES);
              listener.onResponse(
                  new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
            } else {
              SqlExplainLevel level =
                  format == ExplainFormat.COST
                      ? SqlExplainLevel.ALL_ATTRIBUTES
                      : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
              String logical = RelOptUtil.toString(rel, level);
              AtomicReference<String> physical = new AtomicReference<>();
              AtomicReference<String> javaCode = new AtomicReference<>();
              try (Hook.Closeable closeable = getPhysicalPlanInHook(physical, level)) {
                if (format == ExplainFormat.EXTENDED) {
                  getCodegenInHook(javaCode);
                }
                // triggers the hook
                AccessController.doPrivileged(
                    (PrivilegedAction<PreparedStatement>)
                        () -> OpenSearchRelRunners.run(context, rel));
              }
              listener.onResponse(
                  new ExplainResponse(
                      new ExplainResponseNodeV2(logical, physical.get(), javaCode.get())));
            }
          } catch (Exception e) {
            listener.onFailure(e);
          }
        });
  }

  @Override
  public void execute(
      RelNode rel, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Starting");
    System.out.println("OpenSearchExecutionEngine.execute(RelNode) - RelNode: " + rel);
    System.out.println("OpenSearchExecutionEngine.execute(RelNode) - RelNode class: " + rel.getClass().getSimpleName());
    System.out.println("OpenSearchExecutionEngine.execute(RelNode) - RelNode type: " + rel.getRowType());
    System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Context: " + context);
    
    client.schedule(
        () -> {
            System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Inside client.schedule()");
            
            AccessController.doPrivileged(
                (PrivilegedAction<Void>)
                    () -> {
                      try {
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Inside doPrivileged");
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - About to run OpenSearchRelRunners.run()");
                          
                          PreparedStatement statement = OpenSearchRelRunners.run(context, rel);
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Statement created: " + statement);
                          
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - About to execute query");
                          ResultSet result = statement.executeQuery();
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Query executed, got ResultSet: " + result);
                          
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Building result set");
                          buildResultSet(result, rel.getRowType(), listener);
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Result set built and sent to listener");
                      } catch (SQLException e) {
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - SQLException: " + e.getMessage());
                          e.printStackTrace();
                          throw new RuntimeException(e);
                      } catch (Exception e) {
                          System.out.println("OpenSearchExecutionEngine.execute(RelNode) - Exception: " + e.getMessage());
                          e.printStackTrace();
                          throw e;
                      }
                      return null;
                    });
            System.out.println("OpenSearchExecutionEngine.execute(RelNode) - After doPrivileged");
        });
    System.out.println("OpenSearchExecutionEngine.execute(RelNode) - After scheduling");
  }

  private void buildResultSet(
      ResultSet resultSet, RelDataType rowTypes, ResponseListener<QueryResponse> listener)
      throws SQLException {
    System.out.println("OpenSearchExecutionEngine.buildResultSet() - Starting");
    System.out.println("OpenSearchExecutionEngine.buildResultSet() - ResultSet: " + resultSet);
    System.out.println("OpenSearchExecutionEngine.buildResultSet() - RowTypes: " + rowTypes);
    
    try {
      // Get the ResultSet metadata to know about columns
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Getting metadata");
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Column count: " + columnCount);
      
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Getting field types");
      List<RelDataType> fieldTypes =
          rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Field types: " + fieldTypes);
      
      List<ExprValue> values = new ArrayList<>();
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Iterating through ResultSet");
      
      // Iterate through the ResultSet
      int rowCount = 0;
      while (resultSet.next()) {
        rowCount++;
        Map<String, ExprValue> row = new LinkedHashMap<String, ExprValue>();
        // Loop through each column
        for (int i = 1; i <= columnCount; i++) {
          String columnName = metaData.getColumnName(i);
          int sqlType = metaData.getColumnType(i);
          RelDataType fieldType = fieldTypes.get(i - 1);
          System.out.println("OpenSearchExecutionEngine.buildResultSet() - Processing column: " + columnName + 
                            ", SQL type: " + sqlType + ", Field type: " + fieldType);
          
          ExprValue exprValue =
              JdbcOpenSearchDataTypeConvertor.getExprValueFromSqlType(
                  resultSet, i, sqlType, fieldType, columnName);
          row.put(columnName, exprValue);
        }
        values.add(ExprTupleValue.fromExprValueMap(row));
      }
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Processed " + rowCount + " rows");
  
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Building schema");
      List<Column> columns = new ArrayList<>(metaData.getColumnCount());
      for (int i = 1; i <= columnCount; ++i) {
        String columnName = metaData.getColumnName(i);
        RelDataType fieldType = fieldTypes.get(i - 1);
        ExprType exprType = convertRelDataTypeToExprType(fieldType);
        System.out.println("OpenSearchExecutionEngine.buildResultSet() - Adding column: " + columnName + 
                          ", Field type: " + fieldType + ", Expr type: " + exprType);
        columns.add(new Column(columnName, null, exprType));
      }
      Schema schema = new Schema(columns);
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Schema built: " + schema);
      
      QueryResponse response = new QueryResponse(schema, values, null);
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Response created, sending to listener");
      listener.onResponse(response);
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Response sent to listener");
    } catch (Exception e) {
      System.out.println("OpenSearchExecutionEngine.buildResultSet() - Exception: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }
}
