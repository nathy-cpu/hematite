//! Materializing an arena statement as the owned AST.
//!
//! The engine's query layer still reads `crate::parser::ast`, whose nodes own their
//! children. This walks an [`Ast`] and rebuilds that form.
//!
//! It exists to make the arena parser verifiable, not to stay: with it in place the
//! whole test suite — 212 of whose tests drive real SQL end to end — exercises the
//! arena parser and checks its output against the owned trees those tests were
//! written for, without a differential harness of its own. It runs after every
//! parse (`parse_sql_statement`, `parse_expression_fragment`, `sql::script`), so
//! each parse pays for a fresh arena and then for the owned tree on top of it. It
//! comes out when the query layer reads the arena directly, and every allocation it
//! makes goes with it.

use crate::parser::arena::{
    AlterOperation as AAlterOperation, ArenaId, Ast, ColumnDefId, ConstraintId, ExprId,
    Expression as AExpr, InsertSource as AInsertSource, ListId, Literal as ALiteral, NameId,
    SelectId, SelectItemKind, Statement as AStmt, TableConstraint as ATableConstraint, TableRef,
    TableRefId,
};
use crate::parser::ast::*;
use crate::parser::types::LiteralValue;

impl Ast {
    /// The parsed statement, in owned form.
    pub fn to_owned_statement(&self) -> Statement {
        self.owned_statement(self.root())
    }

    fn owned_statement(&self, statement: AStmt) -> Statement {
        match statement {
            AStmt::Begin => Statement::Begin,
            AStmt::Commit => Statement::Commit,
            AStmt::Rollback => Statement::Rollback,
            AStmt::Savepoint(name) => Statement::Savepoint(self.owned_name(name)),
            AStmt::RollbackToSavepoint(name) => {
                Statement::RollbackToSavepoint(self.owned_name(name))
            }
            AStmt::ReleaseSavepoint(name) => Statement::ReleaseSavepoint(self.owned_name(name)),
            AStmt::Explain(inner) => Statement::Explain(ExplainStatement {
                statement: Box::new(self.owned_statement(self.stmt(inner))),
            }),
            AStmt::Describe(table) => Statement::Describe(DescribeStatement {
                table: self.owned_name(table),
            }),
            AStmt::ShowTables => Statement::ShowTables,
            AStmt::ShowIndexes(table) => Statement::ShowIndexes(self.opt_owned_name(table)),
            AStmt::ShowCreateTable(table) => Statement::ShowCreateTable(self.owned_name(table)),
            AStmt::Select(select) => Statement::Select(self.owned_select(select)),
            AStmt::SelectInto { table, query } => Statement::SelectInto(SelectIntoStatement {
                table: self.owned_name(table),
                query: self.owned_select(query),
            }),
            AStmt::Update(id) => {
                let update = self.update(id);
                Statement::Update(UpdateStatement {
                    table: self.owned_name(update.table),
                    target_binding: self.opt_owned_name(update.target_binding),
                    source: update.source.get().map(|id| self.owned_table(id)),
                    assignments: self
                        .list(update.assignments)
                        .map(|id| {
                            let assign = self.assign(id);
                            UpdateAssignment {
                                column: self.owned_name(assign.column),
                                value: self.to_owned_expression(assign.value),
                            }
                        })
                        .collect(),
                    where_clause: self.owned_where(update.where_clause),
                })
            }
            AStmt::Insert(id) => {
                let insert = self.insert(id);
                Statement::Insert(InsertStatement {
                    table: self.owned_name(insert.table),
                    columns: self.owned_names(insert.columns),
                    source: match insert.source {
                        AInsertSource::Values(rows) => InsertSource::Values(
                            self.list(rows)
                                .map(|row| {
                                    self.list(self.row(row))
                                        .map(|expr| self.to_owned_expression(expr))
                                        .collect()
                                })
                                .collect(),
                        ),
                        AInsertSource::Select(select) => {
                            InsertSource::Select(Box::new(self.owned_select(select)))
                        }
                    },
                    on_duplicate: insert.has_on_duplicate.then(|| {
                        self.list(insert.on_duplicate)
                            .map(|id| {
                                let assign = self.assign(id);
                                UpdateAssignment {
                                    column: self.owned_name(assign.column),
                                    value: self.to_owned_expression(assign.value),
                                }
                            })
                            .collect()
                    }),
                })
            }
            AStmt::Delete(id) => {
                let delete = self.delete(id);
                Statement::Delete(DeleteStatement {
                    table: self.owned_name(delete.table),
                    target_binding: self.opt_owned_name(delete.target_binding),
                    source: delete.source.get().map(|id| self.owned_table(id)),
                    where_clause: self.owned_where(delete.where_clause),
                })
            }
            AStmt::Create(id) => {
                let create = self.create(id);
                Statement::Create(CreateStatement {
                    table: self.owned_name(create.table),
                    columns: self
                        .list(create.columns)
                        .map(|id| self.owned_column_def(id))
                        .collect(),
                    constraints: self
                        .list(create.constraints)
                        .map(|id| self.owned_constraint(id))
                        .collect(),
                    if_not_exists: create.if_not_exists,
                })
            }
            AStmt::CreateIndex(id) => {
                let create = self.create_index(id);
                Statement::CreateIndex(CreateIndexStatement {
                    index_name: self.owned_name(create.index_name),
                    table: self.owned_name(create.table),
                    columns: self.owned_names(create.columns),
                    unique: create.unique,
                    if_not_exists: create.if_not_exists,
                })
            }
            AStmt::Alter(id) => {
                let alter = self.alter(id);
                Statement::Alter(AlterStatement {
                    table: self.owned_name(alter.table),
                    operation: self.owned_alter_operation(alter.operation),
                })
            }
            AStmt::Drop(id) => {
                let drop = self.drop_stmt(id);
                Statement::Drop(DropStatement {
                    table: self.owned_name(drop.table),
                    if_exists: drop.if_exists,
                })
            }
            AStmt::DropIndex(id) => {
                let drop = self.drop_index(id);
                Statement::DropIndex(DropIndexStatement {
                    index_name: self.owned_name(drop.index_name),
                    table: self.owned_name(drop.table),
                    if_exists: drop.if_exists,
                })
            }
        }
    }

    fn owned_name(&self, id: NameId) -> String {
        self.name(id).to_string()
    }

    fn opt_owned_name(&self, id: NameId) -> Option<String> {
        self.opt_name(id).map(str::to_string)
    }

    fn owned_names(&self, ids: ListId<NameId>) -> Vec<String> {
        self.list(ids).map(|id| self.owned_name(id)).collect()
    }

    /// A qualified reference, rejoined into the single string the owned AST holds.
    fn owned_column_reference(&self, qualifier: NameId, name: NameId) -> String {
        match self.opt_name(qualifier) {
            Some(qualifier) => format!("{}.{}", qualifier, self.name(name)),
            None => self.owned_name(name),
        }
    }

    fn owned_where(&self, conditions: ListId<ExprId>) -> Option<WhereClause> {
        if conditions.is_empty() {
            return None;
        }
        Some(WhereClause {
            conditions: self
                .list(conditions)
                .map(|expr| self.to_owned_expression(expr))
                .collect(),
        })
    }

    fn owned_select(&self, id: SelectId) -> SelectStatement {
        let select = self.select(id);
        let items: Vec<_> = self.list(select.columns).collect();
        SelectStatement {
            with_clause: self
                .list(select.with_clause)
                .map(|id| {
                    let cte = self.cte(id);
                    CommonTableExpression {
                        name: self.owned_name(cte.name),
                        query: Box::new(self.owned_select(cte.query)),
                    }
                })
                .collect(),
            distinct: select.distinct,
            columns: items
                .iter()
                .map(|&id| self.owned_select_item(self.item(id).kind))
                .collect(),
            column_aliases: items
                .iter()
                .map(|&id| self.opt_owned_name(self.item(id).alias))
                .collect(),
            from: self.owned_table(select.from),
            where_clause: self.owned_where(select.where_clause),
            group_by: self
                .list(select.group_by)
                .map(|expr| self.to_owned_expression(expr))
                .collect(),
            having_clause: self.owned_where(select.having_clause),
            order_by: self
                .list(select.order_by)
                .map(|id| {
                    let order = self.order(id);
                    OrderByItem {
                        column: self.owned_column_reference(order.qualifier, order.name),
                        direction: order.direction,
                    }
                })
                .collect(),
            limit: select.limit,
            offset: select.offset,
            set_operation: select.set_operation.get().map(|id| {
                let set_op = self.set_op(id);
                SetOperation {
                    operator: set_op.operator,
                    right: Box::new(self.owned_select(set_op.right)),
                }
            }),
        }
    }

    fn owned_select_item(&self, kind: SelectItemKind) -> SelectItem {
        match kind {
            SelectItemKind::Wildcard => SelectItem::Wildcard,
            SelectItemKind::Column(expr) => match self.expr(expr) {
                AExpr::Column { qualifier, name } => {
                    SelectItem::Column(self.owned_column_reference(qualifier, name))
                }
                _ => SelectItem::Expression(self.to_owned_expression(expr)),
            },
            SelectItemKind::Expression(expr) => {
                SelectItem::Expression(self.to_owned_expression(expr))
            }
            SelectItemKind::CountAll => SelectItem::CountAll,
            SelectItemKind::Aggregate { function, target } => SelectItem::Aggregate {
                function,
                column: self.owned_column_reference(target.qualifier, target.name),
            },
        }
    }

    fn owned_table(&self, id: TableRefId) -> TableReference {
        match self.table(id) {
            TableRef::Table { name, alias } => {
                TableReference::Table(self.owned_name(name), self.opt_owned_name(alias))
            }
            TableRef::Derived { subquery, alias } => TableReference::Derived {
                subquery: Box::new(self.owned_select(subquery)),
                alias: self.owned_name(alias),
            },
            TableRef::CrossJoin { left, right } => TableReference::CrossJoin(
                Box::new(self.owned_table(left)),
                Box::new(self.owned_table(right)),
            ),
            TableRef::Join {
                kind,
                left,
                right,
                on,
            } => {
                let left = Box::new(self.owned_table(left));
                let right = Box::new(self.owned_table(right));
                let on = self.to_owned_expression(on);
                match kind {
                    crate::parser::arena::JoinKind::Inner => {
                        TableReference::InnerJoin { left, right, on }
                    }
                    crate::parser::arena::JoinKind::Left => {
                        TableReference::LeftJoin { left, right, on }
                    }
                    crate::parser::arena::JoinKind::Right => {
                        TableReference::RightJoin { left, right, on }
                    }
                    crate::parser::arena::JoinKind::FullOuter => {
                        TableReference::FullOuterJoin { left, right, on }
                    }
                }
            }
        }
    }

    /// One expression subtree, in owned form.
    pub fn to_owned_expression(&self, id: ExprId) -> Expression {
        match self.expr(id) {
            AExpr::Column { qualifier, name } => {
                Expression::Column(self.owned_column_reference(qualifier, name))
            }
            AExpr::Literal(lit) => Expression::Literal(self.owned_literal(lit)),
            AExpr::Parameter(index) => Expression::Parameter(index as usize),
            AExpr::ScalarSubquery(select) => {
                Expression::ScalarSubquery(Box::new(self.owned_select(select)))
            }
            AExpr::Cast { expr, target_type } => Expression::Cast {
                expr: Box::new(self.to_owned_expression(expr)),
                target_type: self.ty(target_type),
            },
            AExpr::Case {
                branches,
                else_expr,
            } => Expression::Case {
                branches: self
                    .list(branches)
                    .map(|id| {
                        let branch = self.branch(id);
                        CaseWhenClause {
                            condition: self.to_owned_expression(branch.condition),
                            result: self.to_owned_expression(branch.result),
                        }
                    })
                    .collect(),
                else_expr: else_expr
                    .get()
                    .map(|id| Box::new(self.to_owned_expression(id))),
            },
            AExpr::ScalarFunctionCall { function, args } => Expression::ScalarFunctionCall {
                function,
                args: self
                    .list(args)
                    .map(|id| self.to_owned_expression(id))
                    .collect(),
            },
            AExpr::AggregateCall { function, target } => Expression::AggregateCall {
                function,
                target: if target.name.is_none() {
                    AggregateTarget::All
                } else {
                    AggregateTarget::Column(
                        self.owned_column_reference(target.qualifier, target.name),
                    )
                },
            },
            AExpr::UnaryMinus(expr) => {
                Expression::UnaryMinus(Box::new(self.to_owned_expression(expr)))
            }
            AExpr::UnaryNot(expr) => Expression::UnaryNot(Box::new(self.to_owned_expression(expr))),
            AExpr::Binary {
                left,
                operator,
                right,
            } => Expression::Binary {
                left: Box::new(self.to_owned_expression(left)),
                operator,
                right: Box::new(self.to_owned_expression(right)),
            },
            AExpr::Comparison {
                left,
                operator,
                right,
            } => Expression::Comparison {
                left: Box::new(self.to_owned_expression(left)),
                operator,
                right: Box::new(self.to_owned_expression(right)),
            },
            AExpr::InList {
                expr,
                values,
                is_not,
            } => Expression::InList {
                expr: Box::new(self.to_owned_expression(expr)),
                values: self
                    .list(values)
                    .map(|id| self.to_owned_expression(id))
                    .collect(),
                is_not,
            },
            AExpr::InSubquery {
                expr,
                subquery,
                is_not,
            } => Expression::InSubquery {
                expr: Box::new(self.to_owned_expression(expr)),
                subquery: Box::new(self.owned_select(subquery)),
                is_not,
            },
            AExpr::Between {
                expr,
                lower,
                upper,
                is_not,
            } => Expression::Between {
                expr: Box::new(self.to_owned_expression(expr)),
                lower: Box::new(self.to_owned_expression(lower)),
                upper: Box::new(self.to_owned_expression(upper)),
                is_not,
            },
            AExpr::Like {
                expr,
                pattern,
                is_not,
            } => Expression::Like {
                expr: Box::new(self.to_owned_expression(expr)),
                pattern: Box::new(self.to_owned_expression(pattern)),
                is_not,
            },
            AExpr::Exists { subquery, is_not } => Expression::Exists {
                subquery: Box::new(self.owned_select(subquery)),
                is_not,
            },
            AExpr::NullCheck { expr, is_not } => Expression::NullCheck {
                expr: Box::new(self.to_owned_expression(expr)),
                is_not,
            },
            AExpr::Logical {
                left,
                operator,
                right,
            } => Expression::Logical {
                left: Box::new(self.to_owned_expression(left)),
                operator,
                right: Box::new(self.to_owned_expression(right)),
            },
        }
    }

    fn owned_literal(&self, id: crate::parser::arena::LitId) -> LiteralValue {
        match self.lit(id) {
            ALiteral::Integer(value) => LiteralValue::Integer(value),
            ALiteral::Text(span) => LiteralValue::Text(self.str(span).to_string()),
            ALiteral::Blob(span) => LiteralValue::Blob(self.bytes(span).to_vec()),
            ALiteral::Boolean(value) => LiteralValue::Boolean(value),
            ALiteral::Float(id) => LiteralValue::Float(self.owned_float(id)),
            ALiteral::Null => LiteralValue::Null,
        }
    }

    /// A float literal's normalized decimal text, rebuilt from its digit spans.
    fn owned_float(&self, id: crate::parser::arena::FloatId) -> String {
        let float = self.float(id);
        let integer = self.str(float.integer);
        let fraction = self.str(float.fraction);
        let integer = if integer.is_empty() { "0" } else { integer };

        let mut out = String::new();
        // `-0` normalizes to `0`, which is why the sign is not written first.
        let is_zero = integer == "0" && fraction.is_empty();
        if float.negative && !is_zero {
            out.push('-');
        }
        out.push_str(integer);
        if !fraction.is_empty() {
            out.push('.');
            out.push_str(fraction);
        }
        out
    }

    fn owned_column_def(&self, id: ColumnDefId) -> ColumnDefinition {
        let def = self.coldef(id);
        ColumnDefinition {
            name: self.owned_name(def.name),
            data_type: self.ty(def.data_type),
            character_set: self.opt_owned_name(def.character_set),
            collation: self.opt_owned_name(def.collation),
            nullable: def.nullable,
            primary_key: def.primary_key,
            auto_increment: def.auto_increment,
            unique: def.unique,
            default_value: def.default_value.get().map(|id| self.owned_literal(id)),
            check_constraint: def.check_constraint.get().map(|id| self.owned_check(id)),
            references: def.references.get().map(|id| self.owned_foreign_key(id)),
        }
    }

    fn owned_check(&self, id: ConstraintId) -> CheckConstraintDefinition {
        match self.constraint(id) {
            ATableConstraint::Check { name, expression } => CheckConstraintDefinition {
                name: self.opt_owned_name(name),
                // The catalog persists a check as SQL text, so the expression is
                // rendered here rather than at parse time.
                expression_sql: self.to_owned_expression(expression).to_sql(),
            },
            _ => unreachable!("column check constraint is a check"),
        }
    }

    fn owned_foreign_key(&self, id: ConstraintId) -> ForeignKeyDefinition {
        match self.constraint(id) {
            ATableConstraint::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => ForeignKeyDefinition {
                name: self.opt_owned_name(name),
                columns: self.owned_names(columns),
                referenced_table: self.owned_name(referenced_table),
                referenced_columns: self.owned_names(referenced_columns),
                on_delete,
                on_update,
            },
            _ => unreachable!("column reference constraint is a foreign key"),
        }
    }

    fn owned_constraint(&self, id: ConstraintId) -> TableConstraint {
        match self.constraint(id) {
            ATableConstraint::Check { .. } => TableConstraint::Check(self.owned_check(id)),
            ATableConstraint::Unique { name, columns } => {
                TableConstraint::Unique(UniqueConstraintDefinition {
                    name: self.opt_owned_name(name),
                    columns: self.owned_names(columns),
                })
            }
            ATableConstraint::ForeignKey { .. } => {
                TableConstraint::ForeignKey(self.owned_foreign_key(id))
            }
            ATableConstraint::PrimaryKey { name, columns } => {
                TableConstraint::Unique(UniqueConstraintDefinition {
                    name: self.opt_owned_name(name),
                    columns: self.owned_names(columns),
                })
            }
        }
    }

    fn owned_alter_operation(&self, operation: AAlterOperation) -> AlterOperation {
        match operation {
            AAlterOperation::RenameTo(name) => AlterOperation::RenameTo(self.owned_name(name)),
            AAlterOperation::RenameColumn { old_name, new_name } => AlterOperation::RenameColumn {
                old_name: self.owned_name(old_name),
                new_name: self.owned_name(new_name),
            },
            AAlterOperation::AddColumn(id) => AlterOperation::AddColumn(self.owned_column_def(id)),
            AAlterOperation::AddConstraint(id) => {
                AlterOperation::AddConstraint(self.owned_constraint(id))
            }
            AAlterOperation::DropColumn(name) => AlterOperation::DropColumn(self.owned_name(name)),
            AAlterOperation::DropConstraint(name) => {
                AlterOperation::DropConstraint(self.owned_name(name))
            }
            AAlterOperation::AlterColumnSetDefault { column_name, value } => {
                AlterOperation::AlterColumnSetDefault {
                    column_name: self.owned_name(column_name),
                    default_value: self.owned_literal(value),
                }
            }
            AAlterOperation::AlterColumnDropDefault { column_name } => {
                AlterOperation::AlterColumnDropDefault {
                    column_name: self.owned_name(column_name),
                }
            }
            AAlterOperation::AlterColumnSetNotNull { column_name } => {
                AlterOperation::AlterColumnSetNotNull {
                    column_name: self.owned_name(column_name),
                }
            }
            AAlterOperation::AlterColumnDropNotNull { column_name } => {
                AlterOperation::AlterColumnDropNotNull {
                    column_name: self.owned_name(column_name),
                }
            }
        }
    }
}
