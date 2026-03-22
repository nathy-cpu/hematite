//! Query optimizer for improving query execution plans

use crate::catalog::Schema;
use crate::error::Result;
use crate::query::plan::{IndexType, QueryPlan, SelectAnalysis};

#[derive(Debug, Clone)]
pub struct QueryOptimizer {
    catalog: Schema,
}

impl QueryOptimizer {
    pub fn new(catalog: Schema) -> Self {
        Self { catalog }
    }

    pub fn optimize(&self, plan: QueryPlan) -> Result<QueryPlan> {
        let mut optimized_plan = plan;

        if let Some(analysis) = optimized_plan.select_analysis.clone() {
            // Sanity-check that the analyzed table still exists in the catalog snapshot used by the planner.
            let _table = self
                .catalog
                .get_table_by_name(&analysis.table_name)
                .ok_or_else(|| {
                    crate::error::HematiteError::ParseError(format!(
                        "Table '{}' not found during optimization",
                        analysis.table_name
                    ))
                })?;

            let optimizations = self.optimize_select(&analysis)?;
            let reduction = optimizations.estimated_cost_reduction.clamp(0.0, 0.9);
            optimized_plan.estimated_cost =
                (optimized_plan.estimated_cost * (1.0 - reduction)).max(1.0);
            optimized_plan.optimizations = Some(optimizations);
        }

        Ok(optimized_plan)
    }

    pub fn optimize_select(&self, analysis: &SelectAnalysis) -> Result<SelectOptimizations> {
        let mut optimizations = SelectOptimizations::new();

        // Analyze WHERE clause for optimization opportunities
        self.optimize_where_clause(analysis, &mut optimizations)?;

        // Analyze SELECT clause for optimization opportunities
        self.optimize_select_clause(analysis, &mut optimizations)?;

        // Suggest index usage
        self.suggest_indexes(analysis, &mut optimizations)?;

        Ok(optimizations)
    }

    fn optimize_where_clause(
        &self,
        analysis: &SelectAnalysis,
        optimizations: &mut SelectOptimizations,
    ) -> Result<()> {
        // Check if we can use indexes for WHERE conditions
        for index_usage in &analysis.usable_indexes {
            if matches!(index_usage.index_type, IndexType::PrimaryKey)
                || index_usage.selectivity <= 0.1
            {
                optimizations.recommend_index_scan(index_usage.column_id.clone());
            }
        }

        Ok(())
    }

    fn optimize_select_clause(
        &self,
        analysis: &SelectAnalysis,
        optimizations: &mut SelectOptimizations,
    ) -> Result<()> {
        // Check if we can use covering index
        if analysis.accessed_columns.len() <= 3 {
            // Small number of columns - might benefit from covering index
            optimizations.recommend_covering_index();
        }

        Ok(())
    }

    fn suggest_indexes(
        &self,
        analysis: &SelectAnalysis,
        optimizations: &mut SelectOptimizations,
    ) -> Result<()> {
        // Suggest indexes for frequently accessed columns
        let mut column_access_counts = std::collections::HashMap::new();

        for access in &analysis.accessed_columns {
            *column_access_counts.entry(access.column_id).or_insert(0) += 1;
        }

        for (column_id, count) in column_access_counts {
            if count > 10 {
                // Frequently accessed column - suggest index
                optimizations.suggest_index(column_id);
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SelectOptimizations {
    pub recommended_index_scans: Vec<crate::catalog::ColumnId>,
    pub recommended_covering_index: bool,
    pub suggested_indexes: Vec<crate::catalog::ColumnId>,
    pub estimated_cost_reduction: f64,
}

impl SelectOptimizations {
    pub fn new() -> Self {
        Self {
            recommended_index_scans: Vec::new(),
            recommended_covering_index: false,
            suggested_indexes: Vec::new(),
            estimated_cost_reduction: 0.0,
        }
    }

    pub fn recommend_index_scan(&mut self, column_id: crate::catalog::ColumnId) {
        self.recommended_index_scans.push(column_id);
        self.estimated_cost_reduction += 0.5; // Assume 50% cost reduction
    }

    pub fn recommend_covering_index(&mut self) {
        self.recommended_covering_index = true;
        self.estimated_cost_reduction += 0.2; // Assume 20% cost reduction
    }

    pub fn suggest_index(&mut self, column_id: crate::catalog::ColumnId) {
        self.suggested_indexes.push(column_id);
    }
}
