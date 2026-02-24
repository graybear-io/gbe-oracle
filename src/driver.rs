use gbe_jobs_domain::{
    payloads, subjects, ComponentStarted, JobDefinition, JobId, OrgId, TaskDefinition, TaskId,
};
use gbe_nexus::{dedup_id, EventEmitter};
use std::sync::Arc;
use tracing::warn;

use crate::error::OracleError;
use crate::simple::SimpleOracle;

/// Wraps a `SimpleOracle` with optional event emission for job lifecycle.
///
/// Delegates all DAG logic to the inner `SimpleOracle` (untouched).
/// When an `EventEmitter` is present, emits `JobCreated`, `JobCompleted`,
/// `JobFailed`, and `JobCancelled` events at the appropriate transitions.
/// Emission failures are logged and swallowed.
pub struct OracleDriver {
    oracle: SimpleOracle,
    emitter: Option<Arc<EventEmitter>>,
    job_id: JobId,
    org_id: OrgId,
}

impl OracleDriver {
    pub fn new(
        def: JobDefinition,
        job_id: JobId,
        org_id: OrgId,
        emitter: Option<Arc<EventEmitter>>,
    ) -> Result<Self, OracleError> {
        let oracle = SimpleOracle::new(def)?;
        Ok(Self {
            oracle,
            emitter,
            job_id,
            org_id,
        })
    }

    /// Emit `JobCreated`. Call once after construction.
    pub async fn start(&self) {
        self.emit_job_created().await;
    }

    pub fn ready_tasks(&self) -> Vec<&TaskDefinition> {
        self.oracle.ready_tasks()
    }

    pub fn mark_dispatched(&mut self, task_name: &str) {
        self.oracle.mark_dispatched(task_name);
    }

    /// Record task completion. Returns newly ready tasks.
    /// Emits `JobCompleted` when all tasks are done.
    pub async fn task_completed(&mut self, task_name: &str) -> Vec<TaskDefinition> {
        let newly_ready: Vec<TaskDefinition> = self
            .oracle
            .task_completed(task_name)
            .into_iter()
            .cloned()
            .collect();
        if self.oracle.is_complete() {
            self.emit_job_completed().await;
        }
        newly_ready
    }

    /// Record task failure. Emits `JobFailed`.
    pub async fn task_failed(&mut self, task_name: &str) {
        self.oracle.task_failed(task_name);
        self.emit_job_failed(task_name).await;
    }

    /// Cancel the job externally. Emits `JobCancelled`.
    pub async fn cancel(&mut self, reason: &str) {
        self.oracle.task_failed("__cancelled__");
        self.emit_job_cancelled(reason).await;
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.oracle.is_complete()
    }

    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.oracle.is_failed()
    }

    #[must_use]
    pub fn definition(&self) -> &JobDefinition {
        self.oracle.definition()
    }

    // -- emit helpers --

    async fn emit_job_created(&self) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let def = self.oracle.definition();
        let subject = subjects::jobs::created(&def.job_type);
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "job-created");
        let task_ids: Vec<TaskId> = def
            .tasks
            .iter()
            .filter_map(|t| TaskId::new(&format!("task_{}", t.name)).ok())
            .collect();
        let payload = payloads::JobCreated {
            job_id: self.job_id.clone(),
            org_id: self.org_id.clone(),
            job_type: def.job_type.clone(),
            task_count: def.tasks.len() as u32,
            task_ids,
            created_at: now_millis(),
            definition_ref: None,
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit JobCreated");
        }
    }

    async fn emit_job_completed(&self) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let def = self.oracle.definition();
        let subject = subjects::jobs::completed(&def.job_type);
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "job-completed");
        let payload = payloads::JobCompleted {
            job_id: self.job_id.clone(),
            org_id: self.org_id.clone(),
            job_type: def.job_type.clone(),
            completed_at: now_millis(),
            result_ref: None,
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit JobCompleted");
        }
    }

    async fn emit_job_failed(&self, task_name: &str) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let def = self.oracle.definition();
        let subject = subjects::jobs::failed(&def.job_type);
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "job-failed");
        let task_id = match TaskId::new(&format!("task_{task_name}")) {
            Ok(id) => id,
            Err(_) => return,
        };
        let payload = payloads::JobFailed {
            job_id: self.job_id.clone(),
            org_id: self.org_id.clone(),
            job_type: def.job_type.clone(),
            failed_at: now_millis(),
            failed_task_id: task_id,
            error: format!("task {task_name} failed"),
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit JobFailed");
        }
    }

    async fn emit_job_cancelled(&self, reason: &str) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let def = self.oracle.definition();
        let subject = subjects::jobs::cancelled(&def.job_type);
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "job-cancelled");
        let payload = payloads::JobCancelled {
            job_id: self.job_id.clone(),
            org_id: self.org_id.clone(),
            job_type: def.job_type.clone(),
            cancelled_at: now_millis(),
            reason: reason.to_string(),
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit JobCancelled");
        }
    }
}

/// Emit `ComponentStarted` for the oracle process.
pub async fn emit_started(emitter: &EventEmitter) {
    let subject = subjects::lifecycle::started("oracle");
    let dedup = dedup_id(emitter.component(), emitter.instance_id(), "started");
    let payload = ComponentStarted {
        component: emitter.component().to_string(),
        instance_id: emitter.instance_id().to_string(),
        started_at: now_millis(),
        version: None,
    };
    if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
        warn!(error = %e, subject, "failed to emit ComponentStarted");
    }
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use gbe_jobs_domain::{TaskDefinition, TaskParams, TaskType};
    use gbe_nexus::{
        DomainPayload, MessageHandler, PublishOpts, StreamConfig, SubscribeOpts, Subscription,
        Transport, TransportError,
    };
    use std::sync::Mutex;
    use std::time::Duration;

    struct MockTransport {
        published: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                published: Mutex::new(Vec::new()),
            }
        }

        fn subjects(&self) -> Vec<String> {
            self.published
                .lock()
                .unwrap()
                .iter()
                .map(|(s, _)| s.clone())
                .collect()
        }

        fn payload_at<T: serde::de::DeserializeOwned>(&self, idx: usize) -> DomainPayload<T> {
            let guard = self.published.lock().unwrap();
            DomainPayload::from_bytes(&guard[idx].1).unwrap()
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn publish(
            &self,
            subject: &str,
            payload: Bytes,
            _opts: Option<PublishOpts>,
        ) -> Result<String, TransportError> {
            self.published
                .lock()
                .unwrap()
                .push((subject.to_string(), payload.to_vec()));
            Ok("mock-ack".to_string())
        }

        async fn subscribe(
            &self,
            _subject: &str,
            _group: &str,
            _handler: Box<dyn MessageHandler>,
            _opts: Option<SubscribeOpts>,
        ) -> Result<Box<dyn Subscription>, TransportError> {
            unimplemented!()
        }

        async fn ensure_stream(&self, _config: StreamConfig) -> Result<(), TransportError> {
            unimplemented!()
        }

        async fn trim_stream(
            &self,
            _subject: &str,
            _max_age: Duration,
        ) -> Result<u64, TransportError> {
            unimplemented!()
        }

        async fn ping(&self) -> Result<bool, TransportError> {
            Ok(true)
        }

        async fn close(&self) -> Result<(), TransportError> {
            Ok(())
        }
    }

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

    fn make_emitter(transport: Arc<MockTransport>) -> Arc<EventEmitter> {
        Arc::new(EventEmitter::new(transport, "oracle", "orc-test"))
    }

    #[tokio::test]
    async fn without_emitter_delegates_purely() {
        let job_id = JobId::new("job_test").unwrap();
        let org_id = OrgId::new("org_test").unwrap();
        let mut driver = OracleDriver::new(linear_dag(), job_id, org_id, None).unwrap();

        driver.start().await;
        let ready = driver.ready_tasks();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "a");

        driver.mark_dispatched("a");
        let newly_ready = driver.task_completed("a").await;
        assert_eq!(newly_ready.len(), 1);
        assert_eq!(newly_ready[0].name, "b");

        assert!(!driver.is_complete());
        assert!(!driver.is_failed());
        assert_eq!(driver.definition().name, "Linear");
    }

    #[tokio::test]
    async fn emits_job_created_on_start() {
        let transport = Arc::new(MockTransport::new());
        let emitter = make_emitter(transport.clone());
        let job_id = JobId::new("job_test").unwrap();
        let org_id = OrgId::new("org_test").unwrap();

        let driver = OracleDriver::new(linear_dag(), job_id, org_id, Some(emitter)).unwrap();
        driver.start().await;

        let subjects = transport.subjects();
        assert_eq!(subjects, vec!["gbe.jobs.linear.created"]);

        let payload: DomainPayload<payloads::JobCreated> = transport.payload_at(0);
        assert_eq!(payload.data.job_type, "linear");
        assert_eq!(payload.data.task_count, 3);
        assert_eq!(payload.data.task_ids.len(), 3);
    }

    #[tokio::test]
    async fn emits_job_completed_on_final_task() {
        let transport = Arc::new(MockTransport::new());
        let emitter = make_emitter(transport.clone());
        let job_id = JobId::new("job_test").unwrap();
        let org_id = OrgId::new("org_test").unwrap();

        let mut driver = OracleDriver::new(linear_dag(), job_id, org_id, Some(emitter)).unwrap();
        driver.start().await;

        driver.mark_dispatched("a");
        driver.task_completed("a").await;
        driver.mark_dispatched("b");
        driver.task_completed("b").await;
        driver.mark_dispatched("c");
        driver.task_completed("c").await;

        assert!(driver.is_complete());

        let subjects = transport.subjects();
        assert_eq!(subjects[0], "gbe.jobs.linear.created");
        assert_eq!(*subjects.last().unwrap(), "gbe.jobs.linear.completed");
    }

    #[tokio::test]
    async fn emits_job_failed_on_task_failure() {
        let transport = Arc::new(MockTransport::new());
        let emitter = make_emitter(transport.clone());
        let job_id = JobId::new("job_test").unwrap();
        let org_id = OrgId::new("org_test").unwrap();

        let mut driver = OracleDriver::new(linear_dag(), job_id, org_id, Some(emitter)).unwrap();
        driver.start().await;

        driver.mark_dispatched("a");
        driver.task_failed("a").await;

        assert!(driver.is_failed());

        let subjects = transport.subjects();
        assert_eq!(subjects.len(), 2);
        assert_eq!(subjects[1], "gbe.jobs.linear.failed");

        let payload: DomainPayload<payloads::JobFailed> = transport.payload_at(1);
        assert_eq!(payload.data.error, "task a failed");
    }

    #[tokio::test]
    async fn emits_job_cancelled() {
        let transport = Arc::new(MockTransport::new());
        let emitter = make_emitter(transport.clone());
        let job_id = JobId::new("job_test").unwrap();
        let org_id = OrgId::new("org_test").unwrap();

        let mut driver = OracleDriver::new(linear_dag(), job_id, org_id, Some(emitter)).unwrap();
        driver.start().await;

        driver.cancel("user requested").await;

        assert!(driver.is_failed());
        assert!(driver.ready_tasks().is_empty());

        let subjects = transport.subjects();
        assert_eq!(subjects.len(), 2);
        assert_eq!(subjects[1], "gbe.jobs.linear.cancelled");

        let payload: DomainPayload<payloads::JobCancelled> = transport.payload_at(1);
        assert_eq!(payload.data.reason, "user requested");
    }

    #[tokio::test]
    async fn emit_started_sends_lifecycle_event() {
        let transport = Arc::new(MockTransport::new());
        let emitter = EventEmitter::new(transport.clone(), "oracle", "orc-test");

        emit_started(&emitter).await;

        let subjects = transport.subjects();
        assert_eq!(subjects, vec!["gbe.events.lifecycle.oracle.started"]);

        let payload: DomainPayload<ComponentStarted> = transport.payload_at(0);
        assert_eq!(payload.data.component, "oracle");
        assert_eq!(payload.data.instance_id, "orc-test");
    }
}
