/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** The low level interface of core engine. */
@RequiredArgsConstructor
@AllArgsConstructor
@Log4j2
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;

  @Getter(lazy = true)
  private final CalciteRelNodeVisitor relNodeVisitor = new CalciteRelNodeVisitor();

  private DataSourceService dataSourceService;
  private Settings settings;

  /** Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
      System.out.println("In QueryService.execute()");

    if (shouldUseCalcite(queryType)) {
      executeWithCalcite(plan, queryType, listener);
    } else {
      System.out.println("-----------------------------");
      System.out.println("In QueryService.execute() not using shouldUseCalcite");
      System.out.println(plan);
      System.out.println(queryType);
      System.out.println(listener);
      executeWithLegacy(plan, queryType, listener, Optional.empty());
    }
  }

  /** Explain the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format) {
    System.out.println("QueryService.explain() - Starting");
    System.out.println("QueryService.explain() - Plan type: " + plan.getClass().getSimpleName());
    System.out.println("QueryService.explain() - Plan details: " + plan);
    System.out.println("QueryService.explain() - QueryType: " + queryType);
    System.out.println("QueryService.explain() - Format: " + format);
    System.out.println("QueryService.explain() - Settings: " + settings);
    
    boolean useCalcite = shouldUseCalcite(queryType);
    System.out.println("QueryService.explain() - shouldUseCalcite: " + useCalcite);
    
    if (useCalcite) {
      System.out.println("QueryService.explain() - Using Calcite");
      explainWithCalcite(plan, queryType, listener, format);
    } else {
      System.out.println("QueryService.explain() - Not using Calcite, falling back to legacy");
      System.out.println("QueryService.explain() - Plan: " + plan);
      System.out.println("QueryService.explain() - QueryType: " + queryType);
      System.out.println("QueryService.explain() - Listener: " + listener);
      System.out.println("QueryService.explain() - Format: " + format);
      explainWithLegacy(plan, queryType, listener, format, Optional.empty());
    }
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      System.out.println("QueryService.executeWithCalcite() - Starting");
      System.out.println("QueryService.executeWithCalcite() - Plan type: " + plan.getClass().getSimpleName());
      System.out.println("QueryService.executeWithCalcite() - QueryType: " + queryType);
      System.out.println("QueryService.executeWithCalcite() - Before doPrivileged");
      
      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                try {
                  System.out.println("QueryService.executeWithCalcite() - Inside doPrivileged");
                  System.out.println("QueryService.executeWithCalcite() - Creating CalcitePlanContext");
                  CalcitePlanContext context =
                      CalcitePlanContext.create(buildFrameworkConfig(), queryType);
                  System.out.println("QueryService.executeWithCalcite() - CalcitePlanContext created");
                  
                  System.out.println("QueryService.executeWithCalcite() - Analyzing plan");
                  RelNode relNode = analyze(plan, context);
                  System.out.println("QueryService.executeWithCalcite() - Plan analyzed, relNode: " + relNode);
                  
                  System.out.println("QueryService.executeWithCalcite() - Optimizing relNode");
                  RelNode optimized = optimize(relNode);
                  System.out.println("QueryService.executeWithCalcite() - RelNode optimized: " + optimized);
                  
                  System.out.println("QueryService.executeWithCalcite() - Converting to Calcite plan");
                  RelNode calcitePlan = convertToCalcitePlan(optimized);
                  System.out.println("QueryService.executeWithCalcite() - Calcite plan created: " + calcitePlan);
                  
                  System.out.println("QueryService.executeWithCalcite() - Calling executionEngine.execute()");
                  executionEngine.execute(calcitePlan, context, listener);
                  System.out.println("QueryService.executeWithCalcite() - executionEngine.execute() called");
                  return null;
                } catch (Exception e) {
                  System.out.println("QueryService.executeWithCalcite() - Exception in doPrivileged: " + e.getMessage());
                  e.printStackTrace();
                  throw e;
                }
              });
      System.out.println("QueryService.executeWithCalcite() - After doPrivileged");
    } catch (Throwable t) {
      System.out.println("QueryService.executeWithCalcite() - Caught exception: " + t.getMessage());
      t.printStackTrace();
      
      if (isCalciteFallbackAllowed()) {
        System.out.println("QueryService.executeWithCalcite() - Fallback allowed, falling back to legacy");
        log.warn("Fallback to V2 query engine since got exception", t);
        executeWithLegacy(plan, queryType, listener, Optional.of(t));
      } else {
        System.out.println("QueryService.executeWithCalcite() - Fallback not allowed, failing");
        if (t instanceof Error) {
          // Calcite may throw AssertError during query execution.
          System.out.println("QueryService.executeWithCalcite() - Error instance, creating CalciteUnsupportedException");
          listener.onFailure(new CalciteUnsupportedException(t.getMessage()));
        } else {
          System.out.println("QueryService.executeWithCalcite() - Exception instance, passing through");
          listener.onFailure((Exception) t);
        }
      }
    }
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format) {
    System.out.println("QueryService.explainWithCalcite() - Starting");
    System.out.println("QueryService.explainWithCalcite() - Plan: " + plan);
    System.out.println("QueryService.explainWithCalcite() - QueryType: " + queryType);
    System.out.println("QueryService.explainWithCalcite() - Format: " + format);
    
    try {
      System.out.println("QueryService.explainWithCalcite() - Before doPrivileged");
      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                try {
                  System.out.println("QueryService.explainWithCalcite() - Inside doPrivileged");
                  System.out.println("QueryService.explainWithCalcite() - Creating CalcitePlanContext");
                  CalcitePlanContext context =
                      CalcitePlanContext.create(buildFrameworkConfig(), queryType);
                  System.out.println("QueryService.explainWithCalcite() - CalcitePlanContext created");
                  
                  System.out.println("QueryService.explainWithCalcite() - Analyzing plan");
                  RelNode relNode = analyze(plan, context);
                  System.out.println("QueryService.explainWithCalcite() - Plan analyzed, relNode: " + relNode);
                  
                  System.out.println("QueryService.explainWithCalcite() - Optimizing relNode");
                  RelNode optimized = optimize(relNode);
                  System.out.println("QueryService.explainWithCalcite() - RelNode optimized: " + optimized);
                  
                  System.out.println("QueryService.explainWithCalcite() - Converting to Calcite plan");
                  RelNode calcitePlan = convertToCalcitePlan(optimized);
                  System.out.println("QueryService.explainWithCalcite() - Calcite plan created: " + calcitePlan);
                  
                  System.out.println("QueryService.explainWithCalcite() - Calling executionEngine.explain()");
                  executionEngine.explain(calcitePlan, format, context, listener);
                  System.out.println("QueryService.explainWithCalcite() - executionEngine.explain() called");
                  return null;
                } catch (Exception e) {
                  System.out.println("QueryService.explainWithCalcite() - Exception in doPrivileged: " + e.getMessage());
                  e.printStackTrace();
                  throw e;
                }
              });
      System.out.println("QueryService.explainWithCalcite() - After doPrivileged");
    } catch (Throwable t) {
      System.out.println("QueryService.explainWithCalcite() - Caught exception: " + t.getMessage());
      t.printStackTrace();
      if (isCalciteFallbackAllowed()) {
        System.out.println("QueryService.explainWithCalcite() - Fallback allowed, falling back to legacy");
        log.warn("Fallback to V2 query engine since got exception", t);
        explainWithLegacy(plan, queryType, listener, format, Optional.of(t));
      } else {
        System.out.println("QueryService.explainWithCalcite() - Fallback not allowed, failing");
        if (t instanceof Error) {
          // Calcite may throw AssertError during query execution.
          System.out.println("QueryService.explainWithCalcite() - Error instance, creating CalciteUnsupportedException");
          listener.onFailure(new CalciteUnsupportedException(t.getMessage()));
        } else {
          System.out.println("QueryService.explainWithCalcite() - Exception instance, passing through");
          listener.onFailure((Exception) t);
        }
      }
    }
  }

  public void executeWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      Optional<Throwable> calciteFailure) {
    try {
      System.out.println("-----------------------------");
      System.out.println("Execute with legacy");
      System.out.println("Plan " + plan);
      System.out.println("Type " + queryType);
      System.out.println("Listener " + listener);
      executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      if (shouldUseCalcite(queryType) && isCalciteFallbackAllowed()) {
        // if there is a failure thrown from Calcite and execution after fallback V2
        // keeps failure, we should throw the failure from Calcite.
        calciteFailure.ifPresentOrElse(
            t -> listener.onFailure(new RuntimeException(t)), () -> listener.onFailure(e));
      } else {
        listener.onFailure(e);
      }
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param queryType {@link QueryType}
   * @param listener {@link ResponseListener} for explain response
   * @param calciteFailure Optional failure thrown from calcite
   */
  public void explainWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format,
      Optional<Throwable> calciteFailure) {
    System.out.println("QueryService.explainWithLegacy() - Starting");
    System.out.println("QueryService.explainWithLegacy() - Plan: " + plan);
    System.out.println("QueryService.explainWithLegacy() - QueryType: " + queryType);
    System.out.println("QueryService.explainWithLegacy() - Format: " + format);
    System.out.println("QueryService.explainWithLegacy() - CalciteFailure present: " + calciteFailure.isPresent());
    
    try {
      System.out.println("-----------------------------");
      System.out.println("Explain with legacy");
      System.out.println("Plan " + plan);
      System.out.println("Type " + queryType);
      System.out.println("Listener " + listener);
      
      if (format != null && format != Explain.ExplainFormat.STANDARD) {
        System.out.println("QueryService.explainWithLegacy() - Unsupported format: " + format);
        throw new UnsupportedOperationException(
            "Explain mode " + format.name() + " is not supported in v2 engine");
      }
      
      System.out.println("QueryService.explainWithLegacy() - Analyzing plan");
      LogicalPlan analyzedPlan = analyze(plan, queryType);
      System.out.println("QueryService.explainWithLegacy() - Plan analyzed: " + analyzedPlan);
      
      System.out.println("QueryService.explainWithLegacy() - Planning analyzed plan");
      PhysicalPlan physicalPlan = plan(analyzedPlan);
      System.out.println("QueryService.explainWithLegacy() - Physical plan created: " + physicalPlan);
      
      System.out.println("QueryService.explainWithLegacy() - Calling executionEngine.explain()");
      executionEngine.explain(physicalPlan, listener);
      System.out.println("QueryService.explainWithLegacy() - executionEngine.explain() called");
    } catch (Exception e) {
      System.out.println("QueryService.explainWithLegacy() - Exception: " + e.getMessage());
      e.printStackTrace();
      
      if (shouldUseCalcite(queryType) && isCalciteFallbackAllowed()) {
        System.out.println("QueryService.explainWithLegacy() - Handling exception with calcite fallback");
        // if there is a failure thrown from Calcite and execution after fallback V2
        // keeps failure, we should throw the failure from Calcite.
        calciteFailure.ifPresentOrElse(
            t -> {
              System.out.println("QueryService.explainWithLegacy() - Using calcite failure: " + t.getMessage());
              listener.onFailure(new RuntimeException(t));
            },
            () -> {
              System.out.println("QueryService.explainWithLegacy() - Using current failure: " + e.getMessage());
              listener.onFailure(e);
            });
      } else {
        System.out.println("QueryService.explainWithLegacy() - Passing through exception: " + e.getMessage());
        listener.onFailure(e);
      }
    }
  }

  /**
   * Execute the {@link LogicalPlan}, with {@link PlanContext} and using {@link ResponseListener} to
   * get response.<br>
   * Todo. Pass split from PlanContext to ExecutionEngine in following PR.
   *
   * @param plan {@link LogicalPlan}
   * @param planContext {@link PlanContext}
   * @param listener {@link ResponseListener}
   */
  public void executePlan(
      LogicalPlan plan,
      PlanContext planContext,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      System.out.println("-----------------------------");
      System.out.println("Execute plan");
      System.out.println(plan);
      System.out.println(planContext);
      planContext
          .getSplit()
          .ifPresentOrElse(
              split -> executionEngine.execute(plan(plan), new ExecutionContext(split), listener),
              () ->
                  executionEngine.execute(
                      plan(plan), ExecutionContext.emptyExecutionContext(), listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
    return getRelNodeVisitor().analyze(plan, context);
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan, QueryType queryType) {
    System.out.println("----------------------------------");
    System.out.println("Logical Plan analyze()");
    System.out.println("Plan: " + plan);
    System.out.println("Type: " + queryType);
    return analyzer.analyze(plan, new AnalysisContext(queryType));
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    System.out.println("Planner.plan() received: " + plan);
    return planner.plan(plan);
  }

  public RelNode optimize(RelNode plan) {
    return planner.customOptimize(plan);
  }

  private boolean isCalciteFallbackAllowed() {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
    } else {
      return true;
    }
  }

  private boolean isCalciteEnabled(Settings settings) {
    System.out.println("QueryService.isCalciteEnabled() - Settings: " + settings);
    if (settings != null) {
      Boolean enabled = settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
      System.out.println("QueryService.isCalciteEnabled() - CALCITE_ENGINE_ENABLED: " + enabled);
      return enabled;
    } else {
      System.out.println("QueryService.isCalciteEnabled() - Settings is null, returning false");
      return false;
    }
  }

  // TODO https://github.com/opensearch-project/sql/issues/3457
  // Calcite is not available for SQL query now. Maybe release in 3.1.0?
  private boolean shouldUseCalcite(QueryType queryType) {
    System.out.println("QueryService.shouldUseCalcite() - QueryType: " + queryType);
    boolean calciteEnabled = isCalciteEnabled(settings);
    boolean isTypePPL = queryType == QueryType.PPL;
    System.out.println("QueryService.shouldUseCalcite() - calciteEnabled: " + calciteEnabled);
    System.out.println("QueryService.shouldUseCalcite() - isTypePPL: " + isTypePPL);
    return calciteEnabled && isTypePPL;
  }

  private FrameworkConfig buildFrameworkConfig() {
    System.out.println("buildFrameworkConfig() - Starting to build Calcite framework config");
    
    // Use simple calcite schema since we don't compute tables in advance of the query.
    System.out.println("buildFrameworkConfig() - Creating root schema");
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    System.out.println("buildFrameworkConfig() - Root schema created: " + rootSchema.getClass().getSimpleName());
    
    System.out.println("buildFrameworkConfig() - Creating OpenSearch schema with dataSourceService: " + 
                      (dataSourceService != null ? dataSourceService.getClass().getSimpleName() : "null"));
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));
    System.out.println("buildFrameworkConfig() - OpenSearch schema created: " + opensearchSchema.getClass().getSimpleName());
    
    System.out.println("buildFrameworkConfig() - Building ConfigBuilder");
    Frameworks.ConfigBuilder configBuilder =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT) // TODO check
            .defaultSchema(opensearchSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    System.out.println("buildFrameworkConfig() - ConfigBuilder created: " + configBuilder.getClass().getSimpleName());
    
    System.out.println("buildFrameworkConfig() - Building final FrameworkConfig");
    FrameworkConfig config = configBuilder.build();
    System.out.println("buildFrameworkConfig() - FrameworkConfig built: " + config.getClass().getSimpleName());
    
    return config;
  }

  /**
   * Convert OpenSearch Plan to Calcite Plan. Although both plans consist of Calcite RelNodes, there
   * are some differences in the topological structures or semantics between them.
   *
   * @param osPlan Logical Plan derived from OpenSearch PPL
   */
  private static RelNode convertToCalcitePlan(RelNode osPlan) {
    System.out.println("convertToCalcitePlan() - Starting conversion from OpenSearch plan to Calcite plan");
    System.out.println("convertToCalcitePlan() - Input plan: " + osPlan);
    System.out.println("convertToCalcitePlan() - Input plan class: " + osPlan.getClass().getSimpleName());
    
    RelNode calcitePlan = osPlan;
    
    /* Calcite only ensures collation of the final result produced from the root sort operator.
     * While we expect that the collation can be preserved through the pipes over PPL, we need to
     * explicitly add a sort operator on top of the original plan
     * to ensure the correct collation of the final result.
     * See logic in ${@link CalcitePrepareImpl}
     * For the redundant sort, we rely on Calcite optimizer to eliminate
     */
    System.out.println("convertToCalcitePlan() - Getting collation from trait set");
    RelCollation collation = osPlan.getTraitSet().getCollation();
    System.out.println("convertToCalcitePlan() - Collation: " + collation);
    
    if (!(osPlan instanceof Sort) && collation != RelCollations.EMPTY) {
      System.out.println("convertToCalcitePlan() - Plan is not a Sort and collation is not empty, creating LogicalSort");
      calcitePlan = LogicalSort.create(osPlan, collation, null, null);
      System.out.println("convertToCalcitePlan() - LogicalSort created: " + calcitePlan);
    } else {
      System.out.println("convertToCalcitePlan() - No need to create LogicalSort, using original plan");
    }
    
    System.out.println("convertToCalcitePlan() - Final Calcite plan: " + calcitePlan);
    return calcitePlan;
  }
}
