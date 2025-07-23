/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.executor.QueryType.PPL;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;

/** Build {@link Statement} from PPL Query. */
@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchPPLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitDmlStatement(OpenSearchPPLParser.DmlStatementContext ctx) {
    System.out.println("AstStatementBuilder.visitDmlStatement() - Starting");
    System.out.println("AstStatementBuilder.visitDmlStatement() - Context.isExplain: " + context.isExplain);
    System.out.println("AstStatementBuilder.visitDmlStatement() - Context.format: " + context.format);
    System.out.println("AstStatementBuilder.visitDmlStatement() - Has explainStatement: " + (ctx.explainStatement() != null));
    
    Query query = new Query(addSelectAll(astBuilder.visit(ctx)), context.getFetchSize(), PPL);
    
    Statement result;
    if (ctx.explainStatement() != null) {
      System.out.println("AstStatementBuilder.visitDmlStatement() - Using ctx.explainStatement()");
      if (ctx.explainStatement().explainMode() == null) {
        System.out.println("AstStatementBuilder.visitDmlStatement() - No explainMode, creating Explain");
        result = new Explain(query, PPL);
      } else {
        System.out.println("AstStatementBuilder.visitDmlStatement() - With explainMode: " + ctx.explainStatement().explainMode().getText());
        result = new Explain(query, PPL, ctx.explainStatement().explainMode().getText());
      }
    } else {
      System.out.println("AstStatementBuilder.visitDmlStatement() - No ctx.explainStatement()");
      if (context.isExplain) {
        System.out.println("AstStatementBuilder.visitDmlStatement() - Using context.isExplain, creating Explain");
        result = new Explain(query, PPL, context.format);
      } else {
        System.out.println("AstStatementBuilder.visitDmlStatement() - Not an explain, creating Query");
        result = query;
      }
    }
    
    System.out.println("AstStatementBuilder.visitDmlStatement() - Result type: " + result.getClass().getSimpleName());
    return result;
  }

  @Override
  protected Statement aggregateResult(Statement aggregate, Statement nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  @Data
  @Builder
  public static class StatementBuilderContext {
    private final boolean isExplain;
    private final int fetchSize;
    private final String format;
  }

  private UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else {
      return new Project(ImmutableList.of(AllFields.of())).attach(plan);
    }
  }
}
