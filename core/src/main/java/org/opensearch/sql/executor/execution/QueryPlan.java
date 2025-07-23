/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;

/** Query plan which includes a <em>select</em> query. */
public class QueryPlan extends AbstractPlan {

  /** The query plan ast. */
  protected final UnresolvedPlan plan;

  /** Query service. */
  protected final QueryService queryService;

  protected final ResponseListener<ExecutionEngine.QueryResponse> listener;

  protected final Optional<Integer> pageSize;

  /** Constructor. */
  public QueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
    this.pageSize = Optional.empty();
  }

  /** Constructor with page size. */
  public QueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      int pageSize,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
    this.pageSize = Optional.of(pageSize);
  }

  @Override
  public void execute() {
    System.out.println("inside of execute");
    if (pageSize.isPresent()) {
      System.out.println("inside of execute with page size");
      System.out.println(pageSize.get());
      queryService.execute(new Paginate(pageSize.get(), plan), getQueryType(), listener);
    } else {
      System.out.println("inside of execute without page size");
      System.out.println(plan);
      System.out.println(getQueryType());
      System.out.println(listener);
      queryService.execute(plan, getQueryType(), listener);
      System.out.println("after execute");
    }
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, Explain.ExplainFormat format) {
    System.out.println("QueryPlan.explain() - Starting");
    System.out.println("QueryPlan.explain() - Plan: " + plan);
    System.out.println("QueryPlan.explain() - QueryType: " + getQueryType());
    System.out.println("QueryPlan.explain() - Format: " + format);
    System.out.println("QueryPlan.explain() - QueryService: " + queryService);
    
    if (pageSize.isPresent()) {
      System.out.println("QueryPlan.explain() - Inside explain with page size: " + pageSize.get());
      listener.onFailure(
          new NotImplementedException(
              "`explain` feature for paginated requests is not implemented yet."));
    } else {
      System.out.println("QueryPlan.explain() - Inside explain without page size");
      System.out.println("QueryPlan.explain() - About to call queryService.explain()");
      queryService.explain(plan, getQueryType(), listener, format);
      System.out.println("QueryPlan.explain() - After calling queryService.explain()");
    }
  }
}
