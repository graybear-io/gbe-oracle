use gbe_jobs_domain::{JobDefinition, TaskDefinition};
use std::collections::HashSet;

use crate::error::OracleError;

/// Synchronous DAG state machine. Tracks which tasks have been dispatched
/// and completed, resolves dependencies, and reports ready tasks.
///
/// The `SimpleOracle` is transport-agnostic — it doesn't publish or subscribe.
/// A driver (async loop, bus integration, etc.) calls its methods and handles
/// delivery to operatives.
pub struct SimpleOracle {
    definition: JobDefinition,
    dispatched: HashSet<String>,
    completed: HashSet<String>,
    failed: Option<String>,
}

impl SimpleOracle {
    /// Create a new oracle for a validated job definition.
    ///
    /// # Errors
    ///
    /// Returns `OracleError` if the job definition fails validation.
    pub fn new(def: JobDefinition) -> Result<Self, OracleError> {
        def.validate()?;
        Ok(Self {
            definition: def,
            dispatched: HashSet::new(),
            completed: HashSet::new(),
            failed: None,
        })
    }

    /// Return task definitions whose dependencies are all completed
    /// and that haven't been dispatched yet.
    #[must_use]
    pub fn ready_tasks(&self) -> Vec<&TaskDefinition> {
        if self.failed.is_some() {
            return vec![];
        }
        self.definition
            .tasks
            .iter()
            .filter(|t| {
                !self.dispatched.contains(&t.name)
                    && t.depends_on.iter().all(|dep| self.completed.contains(dep))
            })
            .collect()
    }

    /// Mark a task as dispatched (handed to an operative).
    pub fn mark_dispatched(&mut self, task_name: &str) {
        self.dispatched.insert(task_name.to_string());
    }

    /// Record a task completion. Returns newly ready tasks (if any).
    pub fn task_completed(&mut self, task_name: &str) -> Vec<&TaskDefinition> {
        self.completed.insert(task_name.to_string());
        self.ready_tasks()
    }

    /// Record a task failure. No more tasks will be dispatched.
    pub fn task_failed(&mut self, task_name: &str) {
        self.failed = Some(task_name.to_string());
    }

    /// True when all tasks have completed successfully.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.completed.len() == self.definition.tasks.len()
    }

    /// True if a task has failed.
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }

    /// The name of the failed task, if any.
    #[must_use]
    pub fn failed_task(&self) -> Option<&str> {
        self.failed.as_deref()
    }

    /// The underlying job definition.
    #[must_use]
    pub fn definition(&self) -> &JobDefinition {
        &self.definition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gbe_jobs_domain::{TaskDefinition, TaskParams, TaskType};

    fn linear_dag() -> JobDefinition {
        JobDefinition {
            v: 1,
            name: "Linear".to_string(),
            job_type: "linear".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "a".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "b".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["a".to_string()],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "c".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["b".to_string()],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    fn diamond_dag() -> JobDefinition {
        JobDefinition {
            v: 1,
            name: "Diamond".to_string(),
            job_type: "diamond".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "root".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "left".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "right".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "join".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["left".to_string(), "right".to_string()],
                    params: TaskParams::default(),
                    input_from: std::collections::HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    #[test]
    fn roots_are_initially_ready() {
        let oracle = SimpleOracle::new(linear_dag()).unwrap();
        let ready = oracle.ready_tasks();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "a");
    }

    #[test]
    fn completing_root_unblocks_next() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.mark_dispatched("a");
        let newly_ready = oracle.task_completed("a");
        assert_eq!(newly_ready.len(), 1);
        assert_eq!(newly_ready[0].name, "b");
    }

    #[test]
    fn linear_walks_in_order() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();

        oracle.mark_dispatched("a");
        oracle.task_completed("a");
        oracle.mark_dispatched("b");
        oracle.task_completed("b");
        oracle.mark_dispatched("c");
        oracle.task_completed("c");

        assert!(oracle.is_complete());
    }

    #[test]
    fn diamond_root_unblocks_both_branches() {
        let mut oracle = SimpleOracle::new(diamond_dag()).unwrap();
        oracle.mark_dispatched("root");
        let ready = oracle.task_completed("root");
        let names: Vec<&str> = ready.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"left"));
        assert!(names.contains(&"right"));
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn diamond_join_waits_for_both() {
        let mut oracle = SimpleOracle::new(diamond_dag()).unwrap();
        oracle.mark_dispatched("root");
        oracle.task_completed("root");

        // Dispatch both branches (as a real driver would)
        oracle.mark_dispatched("left");
        oracle.mark_dispatched("right");

        let ready_after_left = oracle.task_completed("left");
        assert!(
            ready_after_left.is_empty(),
            "join not ready with only left done"
        );

        let ready_after_right = oracle.task_completed("right");
        assert_eq!(ready_after_right.len(), 1);
        assert_eq!(ready_after_right[0].name, "join");
    }

    #[test]
    fn failure_stops_dispatch() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.mark_dispatched("a");
        oracle.task_failed("a");

        assert!(oracle.is_failed());
        assert!(oracle.ready_tasks().is_empty());
    }

    #[test]
    fn not_complete_until_all_done() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.mark_dispatched("a");
        oracle.task_completed("a");
        assert!(!oracle.is_complete());
    }

    #[test]
    fn failed_task_name_preserved() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.task_failed("a");
        assert_eq!(oracle.failed_task(), Some("a"));
    }

    #[test]
    fn second_failure_overwrites_first() {
        let mut oracle = SimpleOracle::new(diamond_dag()).unwrap();
        oracle.task_failed("left");
        oracle.task_failed("right");
        // Second call overwrites — current implementation uses Option assignment
        assert_eq!(oracle.failed_task(), Some("right"));
        assert!(oracle.is_failed());
    }

    #[test]
    fn dispatched_but_not_completed_blocks_dependents() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.mark_dispatched("a");
        // "a" dispatched but not completed — "b" should not be ready
        let ready = oracle.ready_tasks();
        assert!(ready.is_empty());
    }

    #[test]
    fn all_roots_dispatched_returns_empty() {
        let mut oracle = SimpleOracle::new(diamond_dag()).unwrap();
        oracle.mark_dispatched("root");
        let ready = oracle.ready_tasks();
        assert!(ready.is_empty());
    }

    #[test]
    fn completing_task_idempotent() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.mark_dispatched("a");
        oracle.task_completed("a");
        oracle.task_completed("a"); // second completion
        assert!(!oracle.is_complete()); // still need b, c
        let ready = oracle.ready_tasks();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "b");
    }

    #[test]
    fn definition_accessor_returns_original() {
        let def = linear_dag();
        let oracle = SimpleOracle::new(def).unwrap();
        assert_eq!(oracle.definition().name, "Linear");
        assert_eq!(oracle.definition().tasks.len(), 3);
    }

    #[test]
    fn partial_diamond_completion_join_waits() {
        let mut oracle = SimpleOracle::new(diamond_dag()).unwrap();
        oracle.mark_dispatched("root");
        oracle.task_completed("root");
        oracle.mark_dispatched("left");
        oracle.mark_dispatched("right");
        oracle.task_completed("left");
        // left done, right dispatched but not complete — join not ready
        let ready = oracle.ready_tasks();
        assert!(ready.is_empty(), "join needs both branches complete");
    }

    fn wide_dag() -> JobDefinition {
        let mut tasks = vec![TaskDefinition {
            name: "root".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: vec![],
            params: TaskParams::default(),
            input_from: std::collections::HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        }];
        for i in 0..10 {
            tasks.push(TaskDefinition {
                name: format!("branch-{i}"),
                task_type: TaskType::new("work").unwrap(),
                depends_on: vec!["root".to_string()],
                params: TaskParams::default(),
                input_from: std::collections::HashMap::new(),
                timeout_secs: None,
                max_retries: None,
            });
        }
        tasks.push(TaskDefinition {
            name: "join".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: (0..10).map(|i| format!("branch-{i}")).collect(),
            params: TaskParams::default(),
            input_from: std::collections::HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        });
        JobDefinition {
            v: 1,
            name: "Wide".to_string(),
            job_type: "wide".to_string(),
            tasks,
        }
    }

    #[test]
    fn wide_fan_out_fan_in() {
        let mut oracle = SimpleOracle::new(wide_dag()).unwrap();
        oracle.mark_dispatched("root");
        let branches = oracle.task_completed("root");
        assert_eq!(branches.len(), 10);

        // Dispatch and complete all but one branch
        for i in 0..10 {
            oracle.mark_dispatched(&format!("branch-{i}"));
        }
        for i in 0..9 {
            oracle.task_completed(&format!("branch-{i}"));
        }
        // Join still not ready — branch-9 dispatched but not complete
        let ready = oracle.ready_tasks();
        assert!(ready.is_empty(), "join needs all 10 branches");

        // Complete last branch
        let ready = oracle.task_completed("branch-9");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "join");
    }

    #[test]
    fn failure_midway_blocks_remaining() {
        let mut oracle = SimpleOracle::new(linear_dag()).unwrap();
        oracle.mark_dispatched("a");
        oracle.task_completed("a");
        oracle.mark_dispatched("b");
        oracle.task_failed("b");

        assert!(oracle.is_failed());
        assert!(!oracle.is_complete());
        assert!(oracle.ready_tasks().is_empty());
        assert_eq!(oracle.failed_task(), Some("b"));
    }

    #[test]
    fn not_failed_initially() {
        let oracle = SimpleOracle::new(linear_dag()).unwrap();
        assert!(!oracle.is_failed());
        assert!(oracle.failed_task().is_none());
    }
}
