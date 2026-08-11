//! MongoDB-backed implementations of the storage ports.
//!
//! One [`MongoStore`] wraps a single [`mongodb::Database`] and implements
//! every durable port — [`CompanyStore`], [`EventLog`], [`MemoryStore`],
//! [`ContextStore`], and [`SecretStore`] — so the same `Arc<MongoStore>` can
//! be injected into all of the `RuntimeBuilder::with_*` setters.
//!
//! ## Multi-tenancy
//!
//! This is the platform backend for hosting many companies on one shared
//! MongoDB cluster (the same cluster the platform's Node backend uses for
//! users/teams/billing). Isolation is layered:
//!
//! - **Database per tenant** (recommended): the hosting layer points each
//!   tenant workload at its own database (`OPENCOMPANY_MONGODB_DB`, e.g.
//!   `oc-<tenant-slug>`), so tenants can never address each other's data and
//!   per-tenant export/drop is a database-level operation.
//! - **Company scoping inside a database**: mirroring the sqlite backend,
//!   every document carries `company_id` and every query filters on it, so a
//!   single database can also host multiple companies (platform mode). The
//!   `owners` collection additionally records the durable company → tenant
//!   mapping for shared-database deployments.
//!
//! ## Semantics
//!
//! Payloads are stored as the same JSON strings the fs/sqlite backends
//! persist, so records round-trip byte-identically across backends and the
//! export/import bundle path works unchanged. Monotonic 0-based sequences are
//! allocated from a `counters` collection with an atomic
//! `findOneAndUpdate {$inc}` per `(company, kind)` key.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::stream::TryStreamExt;
use mongodb::bson::{Document, doc};
use mongodb::options::{FindOneAndUpdateOptions, IndexOptions, ReturnDocument, UpdateOptions};
use mongodb::{Client, Collection, Database, IndexModel};
use tokio::sync::broadcast;

use crate::Result;
use crate::company::CompanyManifest;
use crate::error::OpenCompanyError;
use crate::ports::context::ContextStore;
use crate::ports::events::{EventLog, PruneReport, RetentionPolicy, plan_prune};
use crate::ports::login_codes::LoginCodeRecord;
use crate::ports::memory::MemoryStore;
use crate::ports::now_millis;
use crate::ports::secrets::SecretStore;
use crate::ports::sessions::SessionRecord;
use crate::ports::store::CompanyStore;
use crate::ports::types::{
    ChunkAddr, ChunkHit, ChunkMeta, CompanyEvent, CompanyId, CompanyRecord, CompanySummary,
    CompressedTrace, ContextChunk, EventSeq, EvictionPolicy, LedgerEntry, OverlayBlob, SecretValue,
    StoredEvent, TaskResult,
};
use crate::ports::users::{InviteRecord, UserRecord};
use crate::store::content_address;

fn mongo_err(e: impl std::fmt::Display) -> OpenCompanyError {
    OpenCompanyError::Store(format!("mongodb error: {e}"))
}

/// How a port-contract limit maps onto a MongoDB `find`.
///
/// An enum with a distinct `Empty` arm, rather than the `Option<i64>` this used
/// to be, because the two vocabularies disagree about ZERO and the disagreement
/// is silent. `find().limit(0)` is MongoDB's *no limit* sentinel; every port in
/// `crate::ports` means "an empty page" by a limit of zero, as the fs and sqlite
/// backends implement it. Passing the number straight through therefore returned
/// the WHOLE collection at the exact input where the caller asked for nothing.
///
/// Issue #555: that is how `list_runs` drifted from the other two backends, and
/// it went unseen because `conformance.rs` — which asserts precisely this case —
/// had never run against MongoDB in CI. `read_events_from`, `recent_traces` and
/// `list_inbox` carried the same latent inversion; only the eviction path
/// happened to guard it with an `if n > 0`.
///
/// So the zero case is a variant the compiler makes every call site, present and
/// future, decide about — it cannot be forgotten into the driver's meaning again.
enum FindLimit {
    /// The caller asked for nothing. MUST NOT reach `find().limit()`.
    Empty,
    /// The port contract's "read everything" sentinel (`usize::MAX`), or any
    /// value too large for the driver's `i64`.
    Unlimited,
    /// At most this many documents.
    AtMost(i64),
}

fn find_limit(limit: usize) -> FindLimit {
    match limit {
        0 => FindLimit::Empty,
        n if n > i64::MAX as usize => FindLimit::Unlimited,
        n => FindLimit::AtMost(n as i64),
    }
}

fn get_str(doc: &Document, key: &str) -> Result<String> {
    doc.get_str(key)
        .map(str::to_owned)
        .map_err(|e| mongo_err(format!("missing field {key}: {e}")))
}

fn get_i64(doc: &Document, key: &str) -> Result<i64> {
    doc.get_i64(key)
        .map_err(|e| mongo_err(format!("missing field {key}: {e}")))
}

/// A single MongoDB database implementing all five storage ports.
#[derive(Clone)]
pub struct MongoStore {
    db: Database,
    senders: Arc<StdMutex<HashMap<CompanyId, broadcast::Sender<StoredEvent>>>>,
}

/// How recently a workspace blob must have been uploaded for
/// [`sweep_orphan_blobs`](MongoStore::sweep_orphan_blobs) to leave it alone
/// (issue #664).
///
/// This is the window in which a blob legitimately has no node: `create_binary`
/// uploads the payload and inserts the node as two statements, and a sweep
/// running in between sees a healthy upload as garbage.
///
/// An hour rather than a second because the two failure directions are not
/// symmetric. Too short and a slow or contended write loses its payload with no
/// way to recover it. Too long and an orphan from a genuine crash occupies disk
/// until a later boot reclaims it — which is the outcome this sweep exists to
/// produce anyway, just later. When only one direction is destructive, the
/// margin belongs on the other side.
const INFLIGHT_BLOB_GRACE: Duration = Duration::from_secs(60 * 60);

impl MongoStore {
    /// Connects to `uri` and opens `db_name`, creating the port indexes.
    pub async fn connect(uri: &str, db_name: &str) -> Result<Self> {
        let client = Client::with_uri_str(uri).await.map_err(mongo_err)?;
        Self::from_database(client.database(db_name)).await
    }

    /// Wraps an existing database handle (e.g. for tests), creating indexes.
    pub async fn from_database(db: Database) -> Result<Self> {
        let store = Self {
            db,
            senders: Arc::new(StdMutex::new(HashMap::new())),
        };
        store.ensure_indexes().await?;
        // Reclaim workspace payloads whose node document never landed (issue
        // #553). Best-effort by design: the cost of skipping it is disk that is
        // already unreachable, and the cost of failing boot over it would be a
        // company that will not start. See `sweep_orphan_blobs`.
        match store.sweep_orphan_blobs().await {
            Ok(0) => {}
            Ok(removed) => tracing::info!(
                removed,
                "reclaimed orphaned workspace blobs left by an interrupted write"
            ),
            Err(err) => tracing::warn!(
                error = %err,
                "could not sweep orphaned workspace blobs; they remain until the next boot"
            ),
        }
        Ok(store)
    }

    /// Idempotent index creation — the MongoDB equivalent of the sqlite
    /// backend's `CREATE TABLE IF NOT EXISTS` migrations.
    async fn ensure_indexes(&self) -> Result<()> {
        let unique = |keys: Document| {
            IndexModel::builder()
                .keys(keys)
                .options(IndexOptions::builder().unique(true).build())
                .build()
        };
        // Not every index can be unique: a user holds many sessions, and an
        // address may have several login codes over time.
        let nonunique = |keys: Document| IndexModel::builder().keys(keys).build();
        let plans: [(&str, IndexModel); 30] = [
            ("companies", unique(doc! {"company_id": 1})),
            ("ledger", unique(doc! {"company_id": 1, "idx": 1})),
            ("events", unique(doc! {"company_id": 1, "seq": 1})),
            ("memory_traces", unique(doc! {"company_id": 1, "seq": 1})),
            ("memory_tasks", unique(doc! {"company_id": 1, "task_id": 1})),
            ("context_chunks", unique(doc! {"company_id": 1, "addr": 1})),
            ("secrets", unique(doc! {"company_id": 1, "key": 1})),
            ("inbox", unique(doc! {"company_id": 1, "seq": 1})),
            ("inbox_meta", unique(doc! {"company_id": 1, "key": 1})),
            ("tasks", unique(doc! {"company_id": 1, "task_id": 1})),
            ("facts", unique(doc! {"company_id": 1, "fact_id": 1})),
            ("usage_samples", unique(doc! {"company_id": 1, "seq": 1})),
            ("skill_state", unique(doc! {"company_id": 1, "slug": 1})),
            (
                "workspace_nodes",
                unique(doc! {"company_id": 1, "node_id": 1}),
            ),
            ("users", unique(doc! {"company_id": 1, "user_id": 1})),
            // Enforces one account per address per company, and backs the login
            // lookup.
            ("users", unique(doc! {"company_id": 1, "email": 1})),
            (
                "user_invites",
                unique(doc! {"company_id": 1, "invite_id": 1}),
            ),
            ("user_invites", unique(doc! {"company_id": 1, "email": 1})),
            (
                "user_sessions",
                unique(doc! {"company_id": 1, "session_id": 1}),
            ),
            // Backs session resolution on every authenticated request.
            (
                "user_sessions",
                unique(doc! {"company_id": 1, "token_hash": 1}),
            ),
            (
                "user_sessions",
                nonunique(doc! {"company_id": 1, "user_id": 1}),
            ),
            ("login_codes", unique(doc! {"company_id": 1, "code_id": 1})),
            (
                "login_codes",
                unique(doc! {"company_id": 1, "code_hash": 1}),
            ),
            ("login_codes", nonunique(doc! {"company_id": 1, "email": 1})),
            ("runs", unique(doc! {"company_id": 1, "run_id": 1})),
            // A card has many attempts, and many attempts share a status.
            ("runs", nonunique(doc! {"company_id": 1, "task_id": 1})),
            ("runs", nonunique(doc! {"company_id": 1, "status": 1})),
            (
                "run_steps",
                unique(doc! {"company_id": 1, "run_id": 1, "step_seq": 1}),
            ),
            // Issue #274: one row per snapshot; the compound index backs both the
            // per-workflow list (newest-first) and the prune.
            (
                "workflow_revisions",
                unique(doc! {"company_id": 1, "revision_id": 1}),
            ),
            (
                "workflow_revisions",
                nonunique(doc! {"company_id": 1, "workflow_id": 1, "created_ms": -1}),
            ),
        ];
        for (name, index) in plans {
            self.collection(name)
                .create_index(index)
                .await
                .map_err(mongo_err)?;
        }
        self.collection("owners")
            .create_index(unique(doc! {"company_id": 1}))
            .await
            .map_err(mongo_err)?;
        // Issue #241: the cross-replica arbiter. This unique compound index is
        // what turns two replicas racing one schedule minute into one winning
        // `insert_one` and one `E11000` — the case that actually matters, since
        // hosted replicas share the tenant database. Created outside the array
        // above so adding it does not disturb that array's fixed length.
        self.collection("schedule_fires")
            .create_index(unique(
                doc! {"company_id": 1, "schedule_id": 1, "scheduled_for": 1},
            ))
            .await
            .map_err(mongo_err)?;
        // Issue #596: one durable per-node output snapshot per run. The unique
        // `(company_id, run_id)` index makes the upsert last-write-wins per run;
        // the recency index backs the newest-N prune. Created outside the fixed
        // array above so adding it does not disturb that array's length.
        self.collection("run_outputs")
            .create_index(unique(doc! {"company_id": 1, "run_id": 1}))
            .await
            .map_err(mongo_err)?;
        self.collection("run_outputs")
            .create_index(nonunique(doc! {"company_id": 1, "at_ms": -1}))
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    fn collection(&self, name: &str) -> Collection<Document> {
        self.db.collection::<Document>(name)
    }

    /// Atomically allocates the next 0-based sequence for `(company, kind)`.
    async fn next_seq(&self, id: &CompanyId, kind: &str) -> Result<u64> {
        let counters = self.collection("counters");
        let key = format!("{}:{kind}", id.as_ref());
        let doc = counters
            .find_one_and_update(doc! {"_id": &key}, doc! {"$inc": {"next": 1_i64}})
            .with_options(
                FindOneAndUpdateOptions::builder()
                    .upsert(true)
                    .return_document(ReturnDocument::Before)
                    .build(),
            )
            .await
            .map_err(mongo_err)?;
        // Before the first allocation there is no document: the seq is 0.
        Ok(doc.and_then(|d| d.get_i64("next").ok()).unwrap_or_default() as u64)
    }

    fn sender_for(&self, id: &CompanyId) -> broadcast::Sender<StoredEvent> {
        let mut map = self.senders.lock().expect("sender map poisoned");
        map.entry(id.clone())
            .or_insert_with(|| broadcast::channel(256).0)
            .clone()
    }

    // -- Durable tenant ownership (shared-database platform mode) ----------

    /// Records the owning tenant of a company. Used by platform mode to make
    /// the company → tenant map survive restarts (the in-memory `AppState`
    /// ownership map is hydrated from this at boot).
    pub async fn set_owner(&self, id: &CompanyId, tenant: &str) -> Result<()> {
        self.collection("owners")
            .update_one(
                doc! {"company_id": id.as_ref()},
                doc! {"$set": {"tenant_id": tenant, "updated_ms": now_millis() as i64}},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    /// Removes the ownership record (company deleted).
    pub async fn remove_owner(&self, id: &CompanyId) -> Result<()> {
        self.collection("owners")
            .delete_one(doc! {"company_id": id.as_ref()})
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    /// Every durable company → tenant mapping in this database.
    pub async fn owners(&self) -> Result<Vec<(CompanyId, String)>> {
        let mut cursor = self
            .collection("owners")
            .find(doc! {})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push((
                CompanyId::new(get_str(&doc, "company_id")?),
                get_str(&doc, "tenant_id")?,
            ));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// CompanyStore
// ---------------------------------------------------------------------------

#[async_trait]
impl CompanyStore for MongoStore {
    async fn load(&self, id: &CompanyId) -> Result<Option<CompanyRecord>> {
        let Some(company) = self
            .collection("companies")
            .find_one(doc! {"company_id": id.as_ref()})
            .await
            .map_err(mongo_err)?
        else {
            return Ok(None);
        };
        let manifest: CompanyManifest = toml::from_str(&get_str(&company, "manifest_toml")?)
            .map_err(|e| OpenCompanyError::Store(format!("invalid company.toml: {e}")))?;

        let mut cursor = self
            .collection("ledger")
            .find(doc! {"company_id": id.as_ref()})
            .sort(doc! {"idx": 1})
            .await
            .map_err(mongo_err)?;
        let mut ledger = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            ledger.push(serde_json::from_str::<LedgerEntry>(&get_str(
                &doc,
                "entry_json",
            )?)?);
        }

        let overlay = match company.get_str("overlay_json") {
            Ok(json) => OverlayBlob::parse(json)?,
            Err(_) => OverlayBlob::default(),
        };
        Ok(Some(CompanyRecord {
            id: id.clone(),
            manifest,
            ledger,
            lifecycle: get_str(&company, "lifecycle")?,
            overlay_agents: overlay.agents,
            overlay_desk_members: overlay.desk_members,
            overlay_desk_order: overlay.desk_order,
            overlay_desks: overlay.desks,
            overlay_workflows: overlay.workflows,
            overlay_budgets: overlay.budgets,
            disabled_workflows: overlay.disabled_workflows,
            template_provenance: overlay.provenance,
        }))
    }

    async fn save(&self, record: &CompanyRecord) -> Result<()> {
        let manifest_toml = toml::to_string(&record.manifest)
            .map_err(|e| OpenCompanyError::Store(format!("cannot serialize manifest: {e}")))?;
        let overlay_json = serde_json::to_string(&OverlayBlob::from_record(record))?;
        // Append-only: `save` upserts the company document, never the ledger.
        self.collection("companies")
            .update_one(
                doc! {"company_id": record.id.as_ref()},
                doc! {"$set": {
                    "manifest_toml": manifest_toml,
                    "lifecycle": &record.lifecycle,
                    "overlay_json": overlay_json,
                    "updated_ms": now_millis() as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<CompanySummary>> {
        let mut cursor = self
            .collection("companies")
            .find(doc! {})
            .sort(doc! {"company_id": 1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            let Ok(manifest) = toml::from_str::<CompanyManifest>(&get_str(&doc, "manifest_toml")?)
            else {
                continue;
            };
            out.push(CompanySummary {
                id: CompanyId::new(get_str(&doc, "company_id")?),
                name: manifest.company.name,
                lifecycle: get_str(&doc, "lifecycle")?,
            });
        }
        Ok(out)
    }

    async fn append_ledger(&self, id: &CompanyId, entry: LedgerEntry) -> Result<()> {
        let entry_json = serde_json::to_string(&entry)?;
        let idx = self.next_seq(id, "ledger").await?;
        self.collection("ledger")
            .insert_one(doc! {
                "company_id": id.as_ref(),
                "idx": idx as i64,
                "entry_json": entry_json,
                "at_ms": entry.at_millis as i64,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EventLog
// ---------------------------------------------------------------------------

#[async_trait]
impl EventLog for MongoStore {
    async fn append(&self, id: &CompanyId, event: CompanyEvent) -> Result<EventSeq> {
        let event_json = serde_json::to_string(&event)?;
        let at_millis = now_millis();
        let seq = self.next_seq(id, "events").await?;
        self.collection("events")
            .insert_one(doc! {
                "company_id": id.as_ref(),
                "seq": seq as i64,
                "event_json": event_json,
                "at_ms": at_millis as i64,
            })
            .await
            .map_err(mongo_err)?;

        let stored = StoredEvent {
            seq: EventSeq::new(seq),
            company: id.clone(),
            event,
            at_millis,
        };
        // Best-effort fan-out; an error only means no live subscribers.
        let _ = self.sender_for(id).send(stored);
        Ok(EventSeq::new(seq))
    }

    async fn read_from(
        &self,
        id: &CompanyId,
        seq: EventSeq,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let events = self.collection("events");
        let mut find = events
            .find(doc! {
                "company_id": id.as_ref(),
                "seq": {"$gte": seq.value() as i64},
            })
            .sort(doc! {"seq": 1});
        match find_limit(limit) {
            FindLimit::Empty => return Ok(Vec::new()),
            FindLimit::Unlimited => {}
            FindLimit::AtMost(n) => find = find.limit(n),
        }
        let mut cursor = find.await.map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(StoredEvent {
                seq: EventSeq::new(get_i64(&doc, "seq")? as u64),
                company: id.clone(),
                event: serde_json::from_str(&get_str(&doc, "event_json")?)?,
                at_millis: get_i64(&doc, "at_ms")? as u64,
            });
        }
        Ok(out)
    }

    fn subscribe(&self, id: &CompanyId) -> BoxStream<'static, StoredEvent> {
        let rx = self.sender_for(id).subscribe();
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => return Some((event, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Box::pin(stream)
    }

    async fn prune(&self, id: &CompanyId, policy: &RetentionPolicy) -> Result<PruneReport> {
        // Same whole-log read as the sqlite backend, and for the same reason:
        // the decision belongs to `plan_prune` so the three backends cannot
        // disagree about what a policy means.
        let all = self.read_from(id, EventSeq::new(0), usize::MAX).await?;
        let doomed = plan_prune(&all, policy);

        let mut report = PruneReport {
            scanned: all.len(),
            removed: 0,
            oldest_retained: all.iter().map(|e| e.seq).min(),
        };
        if doomed.is_empty() {
            return Ok(report);
        }

        let seqs: Vec<i64> = doomed.iter().map(|s| s.value() as i64).collect();
        self.collection("events")
            .delete_many(doc! {
                "company_id": id.as_ref(),
                "seq": {"$in": seqs},
            })
            .await
            .map_err(mongo_err)?;

        report.removed = doomed.len();
        report.oldest_retained = all
            .iter()
            .map(|e| e.seq)
            .filter(|seq| doomed.binary_search(seq).is_err())
            .min();
        Ok(report)
    }
}

// ---------------------------------------------------------------------------
// MemoryStore
// ---------------------------------------------------------------------------

#[async_trait]
impl MemoryStore for MongoStore {
    async fn save_trace(&self, id: &CompanyId, trace: CompressedTrace) -> Result<()> {
        let trace_json = serde_json::to_string(&trace)?;
        let seq = self.next_seq(id, "memory_traces").await?;
        self.collection("memory_traces")
            .insert_one(doc! {
                "company_id": id.as_ref(),
                "seq": seq as i64,
                "trace_json": trace_json,
                "at_ms": trace.at_millis as i64,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn recent_traces(&self, id: &CompanyId, limit: usize) -> Result<Vec<CompressedTrace>> {
        let traces = self.collection("memory_traces");
        let mut find = traces
            .find(doc! {"company_id": id.as_ref()})
            .sort(doc! {"seq": -1});
        match find_limit(limit) {
            FindLimit::Empty => return Ok(Vec::new()),
            FindLimit::Unlimited => {}
            FindLimit::AtMost(n) => find = find.limit(n),
        }
        let mut cursor = find.await.map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str::<CompressedTrace>(&get_str(
                &doc,
                "trace_json",
            )?)?);
        }
        // Query returned newest-first; the port contract is newest-last.
        out.reverse();
        Ok(out)
    }

    async fn save_task_result(&self, id: &CompanyId, result: TaskResult) -> Result<()> {
        let result_json = serde_json::to_string(&result)?;
        self.collection("memory_tasks")
            .update_one(
                doc! {"company_id": id.as_ref(), "task_id": &result.task_id},
                doc! {"$set": {
                    "result_json": result_json,
                    "at_ms": now_millis() as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn evict(&self, id: &CompanyId, policy: EvictionPolicy) -> Result<u64> {
        let traces = self.collection("memory_traces");
        let removed = match policy {
            EvictionPolicy::KeepRecent { n } => {
                // Collect the seqs to keep (newest n), delete the rest.
                //
                // `KeepRecent { n: 0 }` keeps nothing, so there is no query to
                // run — and must never become `find().limit(0)`, which would
                // keep EVERYTHING and evict none of it. This arm is the old
                // `if n > 0` guard, now stated in the shared vocabulary.
                let mut keep = Vec::new();
                match find_limit(n) {
                    FindLimit::Empty => {}
                    limit => {
                        let mut find = traces
                            .find(doc! {"company_id": id.as_ref()})
                            .sort(doc! {"seq": -1});
                        if let FindLimit::AtMost(n) = limit {
                            find = find.limit(n);
                        }
                        let mut cursor = find.await.map_err(mongo_err)?;
                        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
                            keep.push(get_i64(&doc, "seq")?);
                        }
                    }
                }
                traces
                    .delete_many(doc! {
                        "company_id": id.as_ref(),
                        "seq": {"$nin": keep},
                    })
                    .await
                    .map_err(mongo_err)?
                    .deleted_count
            }
            EvictionPolicy::OlderThan { before_millis } => {
                traces
                    .delete_many(doc! {
                        "company_id": id.as_ref(),
                        "at_ms": {"$lt": before_millis as i64},
                    })
                    .await
                    .map_err(mongo_err)?
                    .deleted_count
            }
        };
        Ok(removed)
    }
}

// ---------------------------------------------------------------------------
// ContextStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ContextStore for MongoStore {
    async fn put(&self, id: &CompanyId, chunk: ContextChunk) -> Result<ChunkAddr> {
        let addr = content_address(&chunk.body);
        // Insertion order stands in for the sqlite backend's rowid ordering.
        let ord = self.next_seq(id, "context_ord").await?;
        let result = self
            .collection("context_chunks")
            .update_one(
                doc! {"company_id": id.as_ref(), "addr": &addr},
                doc! {"$setOnInsert": {
                    "label": &chunk.label,
                    "body": &chunk.body,
                    "len": chunk.body.len() as i64,
                    "ord": ord as i64,
                    "stored_ms": now_millis() as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await;
        match result {
            Ok(_) => Ok(ChunkAddr::new(addr)),
            Err(e) => Err(mongo_err(e)),
        }
    }

    async fn list(&self, id: &CompanyId, prefix: &str) -> Result<Vec<ChunkMeta>> {
        let mut cursor = self
            .collection("context_chunks")
            .find(doc! {"company_id": id.as_ref()})
            .sort(doc! {"ord": 1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            let label = get_str(&doc, "label")?;
            if label.starts_with(prefix) {
                out.push(ChunkMeta {
                    addr: ChunkAddr::new(get_str(&doc, "addr")?),
                    label,
                    len: get_i64(&doc, "len")? as usize,
                    // Absent on documents written before the field existed;
                    // those read as an unknown (`0`) store time rather than
                    // failing the whole list.
                    stored_at_millis: doc.get_i64("stored_ms").unwrap_or(0).max(0) as u64,
                });
            }
        }
        Ok(out)
    }

    async fn peek(
        &self,
        id: &CompanyId,
        addr: &ChunkAddr,
        range: Option<Range<usize>>,
    ) -> Result<String> {
        let doc = self
            .collection("context_chunks")
            .find_one(doc! {"company_id": id.as_ref(), "addr": addr.as_ref()})
            .await
            .map_err(mongo_err)?
            .ok_or_else(|| {
                OpenCompanyError::Store(format!("context chunk not found: {}", addr.as_ref()))
            })?;
        let body = get_str(&doc, "body")?;
        match range {
            None => Ok(body),
            Some(r) => {
                let start = r.start.min(body.len());
                let end = r.end.min(body.len());
                if start >= end {
                    return Ok(String::new());
                }
                Ok(body[start..end].to_string())
            }
        }
    }

    async fn search(&self, id: &CompanyId, query: &str, limit: usize) -> Result<Vec<ChunkHit>> {
        let mut cursor = self
            .collection("context_chunks")
            .find(doc! {"company_id": id.as_ref()})
            .sort(doc! {"ord": 1})
            .await
            .map_err(mongo_err)?;
        let mut hits = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            if hits.len() >= limit {
                break;
            }
            let body = get_str(&doc, "body")?;
            if let Some(pos) = body.find(query) {
                let start = pos.saturating_sub(24);
                let end = (pos + query.len() + 24).min(body.len());
                hits.push(ChunkHit {
                    addr: ChunkAddr::new(get_str(&doc, "addr")?),
                    snippet: body[start..end].to_string(),
                    score: 1.0,
                });
            }
        }
        Ok(hits)
    }
}

// ---------------------------------------------------------------------------
// SecretStore
// ---------------------------------------------------------------------------

#[async_trait]
impl SecretStore for MongoStore {
    async fn get(&self, company: &CompanyId, key: &str) -> Result<Option<SecretValue>> {
        let doc = self
            .collection("secrets")
            .find_one(doc! {"company_id": company.as_ref(), "key": key})
            .await
            .map_err(mongo_err)?;
        doc.map(|d| get_str(&d, "value").map(SecretValue))
            .transpose()
    }

    async fn set(&self, company: &CompanyId, key: &str, value: SecretValue) -> Result<()> {
        self.collection("secrets")
            .update_one(
                doc! {"company_id": company.as_ref(), "key": key},
                doc! {"$set": {"value": value.expose()}},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InboxStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::inbox::InboxStore for MongoStore {
    async fn inboxes(&self, company: &CompanyId) -> Result<Vec<crate::ports::inbox::InboxMeta>> {
        use std::collections::BTreeMap;
        let mut out: BTreeMap<String, crate::ports::inbox::InboxMeta> = BTreeMap::new();
        // Explicit metadata first.
        let mut cursor = self
            .collection("inbox_meta")
            .find(doc! {"company_id": company.as_ref()})
            .await
            .map_err(mongo_err)?;
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            let meta: crate::ports::inbox::InboxMeta =
                serde_json::from_str(&get_str(&doc, "meta_json")?)?;
            out.insert(meta.key.clone(), meta);
        }
        // Synthesize a default enabled meta for message-only inboxes.
        let names = self
            .collection("inbox")
            .distinct("inbox", doc! {"company_id": company.as_ref()})
            .await
            .map_err(mongo_err)?;
        for name in names
            .into_iter()
            .filter_map(|b| b.as_str().map(str::to_string))
        {
            out.entry(name.clone())
                .or_insert_with(|| crate::ports::inbox::InboxMeta {
                    key: name.clone(),
                    name: name.clone(),
                    address: String::new(),
                    enabled: true,
                });
        }
        Ok(out.into_values().collect())
    }

    async fn set_enabled(
        &self,
        company: &CompanyId,
        key: &str,
        meta: &crate::ports::inbox::InboxMeta,
    ) -> Result<()> {
        let meta_json = serde_json::to_string(meta)?;
        self.collection("inbox_meta")
            .update_one(
                doc! {"company_id": company.as_ref(), "key": key},
                doc! {"$set": {"meta_json": meta_json}},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn append(
        &self,
        company: &CompanyId,
        msg: &crate::ports::inbox::EmailRecord,
    ) -> Result<()> {
        let record_json = serde_json::to_string(msg)?;
        let seq = self.next_seq(company, "inbox").await?;
        self.collection("inbox")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "seq": seq as i64,
                "inbox": &msg.inbox,
                "record_json": record_json,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn messages(
        &self,
        company: &CompanyId,
        key: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::ports::inbox::EmailRecord>> {
        let collection = self.collection("inbox");
        let mut find = collection
            .find(doc! {"company_id": company.as_ref(), "inbox": key})
            .sort(doc! {"seq": 1})
            .skip(offset as u64);
        match find_limit(limit) {
            FindLimit::Empty => return Ok(Vec::new()),
            FindLimit::Unlimited => {}
            FindLimit::AtMost(n) => find = find.limit(n),
        }
        let mut cursor = find.await.map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "record_json")?)?);
        }
        Ok(out)
    }

    async fn mark_read(
        &self,
        company: &CompanyId,
        key: &str,
        ids: Option<&[String]>,
    ) -> Result<u64> {
        use crate::ports::inbox::EmailRecord;
        let coll = self.collection("inbox");
        let mut cursor = coll
            .find(doc! {"company_id": company.as_ref(), "inbox": key})
            .await
            .map_err(mongo_err)?;
        let mut unread = 0u64;
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            let seq = get_i64(&doc, "seq")?;
            let mut record: EmailRecord = serde_json::from_str(&get_str(&doc, "record_json")?)?;
            let hit = match ids {
                Some(ids) => ids.iter().any(|id| id == &record.id),
                None => true,
            };
            if hit && !record.read {
                record.read = true;
                coll.update_one(
                    doc! {"company_id": company.as_ref(), "seq": seq},
                    doc! {"$set": {"record_json": serde_json::to_string(&record)?}},
                )
                .await
                .map_err(mongo_err)?;
            }
            if !record.read {
                unread += 1;
            }
        }
        Ok(unread)
    }
}

// ---------------------------------------------------------------------------
// TaskStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::tasks::TaskStore for MongoStore {
    async fn list(&self, company: &CompanyId) -> Result<Vec<crate::ports::tasks::TaskRecord>> {
        let mut cursor = self
            .collection("tasks")
            .find(doc! {"company_id": company.as_ref()})
            .sort(doc! {"updated_ms": -1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "task_json")?)?);
        }
        Ok(out)
    }

    async fn upsert(
        &self,
        company: &CompanyId,
        task: &crate::ports::tasks::TaskRecord,
    ) -> Result<()> {
        self.collection("tasks")
            .update_one(
                doc! {"company_id": company.as_ref(), "task_id": &task.id},
                doc! {"$set": {
                    "task_json": serde_json::to_string(task)?,
                    "updated_ms": task.updated_at_millis as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn delete(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let res = self
            .collection("tasks")
            .delete_one(doc! {"company_id": company.as_ref(), "task_id": id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }
}

// ---------------------------------------------------------------------------
// UserStore
// ---------------------------------------------------------------------------

/// Whether a driver failure is a duplicate-key violation (E11000).
///
/// The unique email/token indexes are the real enforcement; this maps the
/// driver's error onto the crate's `409 Conflict` so every backend reports a
/// clash identically.
fn is_duplicate_key(e: &mongodb::error::Error) -> bool {
    matches!(
        *e.kind,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(ref we))
            if we.code == 11000
    )
}

#[async_trait]
impl crate::ports::users::UserStore for MongoStore {
    async fn list_users(&self, company: &CompanyId) -> Result<Vec<UserRecord>> {
        let mut cursor = self
            .collection("users")
            .find(doc! {"company_id": company.as_ref()})
            .sort(doc! {"created_ms": -1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "user_json")?)?);
        }
        Ok(out)
    }

    async fn get_user(&self, company: &CompanyId, id: &str) -> Result<Option<UserRecord>> {
        let found = self
            .collection("users")
            .find_one(doc! {"company_id": company.as_ref(), "user_id": id})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "user_json")?)?)),
            None => Ok(None),
        }
    }

    async fn find_user_by_email(
        &self,
        company: &CompanyId,
        email: &str,
    ) -> Result<Option<UserRecord>> {
        // Exact match on the unique email index. Normalization is the caller's
        // job, so a store never matches an address it was not asked for.
        let found = self
            .collection("users")
            .find_one(doc! {"company_id": company.as_ref(), "email": email})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "user_json")?)?)),
            None => Ok(None),
        }
    }

    async fn upsert_user(&self, company: &CompanyId, user: &UserRecord) -> Result<()> {
        self.collection("users")
            .update_one(
                doc! {"company_id": company.as_ref(), "user_id": &user.id},
                doc! {"$set": {
                    "email": &user.email,
                    "user_json": serde_json::to_string(user)?,
                    "created_ms": user.created_at_millis as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|e| {
                if is_duplicate_key(&e) {
                    OpenCompanyError::Conflict(format!(
                        "another user already has the email {}",
                        user.email
                    ))
                } else {
                    mongo_err(e)
                }
            })?;
        Ok(())
    }

    async fn delete_user(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let res = self
            .collection("users")
            .delete_one(doc! {"company_id": company.as_ref(), "user_id": id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }

    async fn list_invites(&self, company: &CompanyId) -> Result<Vec<InviteRecord>> {
        let mut cursor = self
            .collection("user_invites")
            .find(doc! {"company_id": company.as_ref()})
            .sort(doc! {"created_ms": -1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "invite_json")?)?);
        }
        Ok(out)
    }

    async fn find_invite_by_email(
        &self,
        company: &CompanyId,
        email: &str,
    ) -> Result<Option<InviteRecord>> {
        let found = self
            .collection("user_invites")
            .find_one(doc! {"company_id": company.as_ref(), "email": email})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "invite_json")?)?)),
            None => Ok(None),
        }
    }

    async fn upsert_invite(&self, company: &CompanyId, invite: &InviteRecord) -> Result<()> {
        self.collection("user_invites")
            .update_one(
                doc! {"company_id": company.as_ref(), "invite_id": &invite.id},
                doc! {"$set": {
                    "email": &invite.email,
                    "invite_json": serde_json::to_string(invite)?,
                    "created_ms": invite.created_at_millis as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|e| {
                if is_duplicate_key(&e) {
                    OpenCompanyError::Conflict(format!("{} is already invited", invite.email))
                } else {
                    mongo_err(e)
                }
            })?;
        Ok(())
    }

    async fn delete_invite(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let res = self
            .collection("user_invites")
            .delete_one(doc! {"company_id": company.as_ref(), "invite_id": id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }
}

// ---------------------------------------------------------------------------
// SessionStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::sessions::SessionStore for MongoStore {
    async fn create(&self, company: &CompanyId, session: &SessionRecord) -> Result<()> {
        self.collection("user_sessions")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "session_id": &session.id,
                "token_hash": &session.token_hash,
                "user_id": &session.user_id,
                "session_json": serde_json::to_string(session)?,
                "created_ms": session.created_at_millis as i64,
                "expires_ms": session.expires_at_millis as i64,
            })
            .await
            .map_err(|e| {
                if is_duplicate_key(&e) {
                    OpenCompanyError::Conflict("that session token already exists".to_string())
                } else {
                    mongo_err(e)
                }
            })?;
        Ok(())
    }

    async fn find_by_token_hash(
        &self,
        company: &CompanyId,
        token_hash: &str,
    ) -> Result<Option<SessionRecord>> {
        let found = self
            .collection("user_sessions")
            .find_one(doc! {"company_id": company.as_ref(), "token_hash": token_hash})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "session_json")?)?)),
            None => Ok(None),
        }
    }

    async fn list_for_user(
        &self,
        company: &CompanyId,
        user_id: &str,
    ) -> Result<Vec<SessionRecord>> {
        let mut cursor = self
            .collection("user_sessions")
            .find(doc! {"company_id": company.as_ref(), "user_id": user_id})
            .sort(doc! {"created_ms": -1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "session_json")?)?);
        }
        Ok(out)
    }

    async fn delete(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let res = self
            .collection("user_sessions")
            .delete_one(doc! {"company_id": company.as_ref(), "session_id": id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }

    async fn delete_for_user(&self, company: &CompanyId, user_id: &str) -> Result<u64> {
        let res = self
            .collection("user_sessions")
            .delete_many(doc! {"company_id": company.as_ref(), "user_id": user_id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count)
    }

    async fn purge_expired(&self, company: &CompanyId, now_millis: u64) -> Result<u64> {
        // Expiry is exclusive: a session expiring exactly at `now` is dead.
        let res = self
            .collection("user_sessions")
            .delete_many(doc! {
                "company_id": company.as_ref(),
                "expires_ms": {"$lte": now_millis as i64},
            })
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count)
    }
}

// ---------------------------------------------------------------------------
// LoginCodeStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::login_codes::LoginCodeStore for MongoStore {
    async fn create(&self, company: &CompanyId, code: &LoginCodeRecord) -> Result<()> {
        self.collection("login_codes")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "code_id": &code.id,
                "code_hash": &code.code_hash,
                "email": &code.email,
                "code_json": serde_json::to_string(code)?,
                "expires_ms": code.expires_at_millis as i64,
                // Promoted so redeemability is expressible as a query filter,
                // which is what makes the claim below atomic.
                "consumed_ms": Option::<i64>::None,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn latest_for_email(
        &self,
        company: &CompanyId,
        email: &str,
    ) -> Result<Option<LoginCodeRecord>> {
        // Served by the non-unique (company_id, email) index. `expires_ms`
        // orders by mint time, since every code for one address shares a TTL.
        let found = self
            .collection("login_codes")
            .find_one(doc! {"company_id": company.as_ref(), "email": email})
            .sort(doc! {"expires_ms": -1})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "code_json")?)?)),
            None => Ok(None),
        }
    }

    async fn consume(
        &self,
        company: &CompanyId,
        code_hash: &str,
        now_millis: u64,
    ) -> Result<Option<LoginCodeRecord>> {
        // Single-use lives or dies here. `findOneAndUpdate` matches and marks in
        // one atomic server-side operation, so of two requests racing on the
        // same code exactly one can match `consumed_ms: null` — the loser's
        // filter no longer matches and it gets `None`.
        //
        // `consumed_ms` is promoted out of the payload purely so the filter can
        // express "unconsumed"; the payload is still the record, and it is
        // brought back into agreement immediately below.
        let claimed = self
            .collection("login_codes")
            .find_one_and_update(
                doc! {
                    "company_id": company.as_ref(),
                    "code_hash": code_hash,
                    "consumed_ms": Option::<i64>::None,
                    "expires_ms": {"$gt": now_millis as i64},
                },
                doc! {"$set": {"consumed_ms": now_millis as i64}},
            )
            .with_options(
                FindOneAndUpdateOptions::builder()
                    .return_document(ReturnDocument::Before)
                    .build(),
            )
            .await
            .map_err(mongo_err)?;
        let Some(doc) = claimed else {
            return Ok(None);
        };
        let mut code: LoginCodeRecord = serde_json::from_str(&get_str(&doc, "code_json")?)?;
        code.consumed_at_millis = Some(now_millis);
        // Payload fidelity: the claim above already guaranteed single use, so a
        // failure here cannot hand out a second session — the code is spent
        // either way.
        self.collection("login_codes")
            .update_one(
                doc! {"company_id": company.as_ref(), "code_hash": code_hash},
                doc! {"$set": {"code_json": serde_json::to_string(&code)?}},
            )
            .await
            .map_err(mongo_err)?;
        Ok(Some(code))
    }

    async fn delete_for_email(&self, company: &CompanyId, email: &str) -> Result<u64> {
        let res = self
            .collection("login_codes")
            .delete_many(doc! {"company_id": company.as_ref(), "email": email})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count)
    }

    async fn purge_expired(&self, company: &CompanyId, now_millis: u64) -> Result<u64> {
        let res = self
            .collection("login_codes")
            .delete_many(doc! {
                "company_id": company.as_ref(),
                "expires_ms": {"$lte": now_millis as i64},
            })
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count)
    }
}

// ---------------------------------------------------------------------------
// FactStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::facts::FactStore for MongoStore {
    async fn list(
        &self,
        company: &CompanyId,
        query: Option<&str>,
        kind: Option<crate::ports::facts::FactKind>,
    ) -> Result<Vec<crate::ports::facts::FactRecord>> {
        let mut cursor = self
            .collection("facts")
            .find(doc! {"company_id": company.as_ref()})
            .sort(doc! {"updated_ms": -1})
            .await
            .map_err(mongo_err)?;
        let mut out: Vec<crate::ports::facts::FactRecord> = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "fact_json")?)?);
        }
        if let Some(kind) = kind {
            out.retain(|f| f.kind == kind);
        }
        if let Some(q) = query.map(str::to_lowercase).filter(|q| !q.is_empty()) {
            out.retain(|f| {
                f.title.to_lowercase().contains(&q) || f.body.to_lowercase().contains(&q)
            });
        }
        Ok(out)
    }

    async fn upsert(
        &self,
        company: &CompanyId,
        fact: &crate::ports::facts::FactRecord,
    ) -> Result<()> {
        self.collection("facts")
            .update_one(
                doc! {"company_id": company.as_ref(), "fact_id": &fact.id},
                doc! {"$set": {
                    "fact_json": serde_json::to_string(fact)?,
                    "updated_ms": fact.updated_at_millis as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn delete(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let res = self
            .collection("facts")
            .delete_one(doc! {"company_id": company.as_ref(), "fact_id": id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }
}

// ---------------------------------------------------------------------------
// ArtifactStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::artifacts::ArtifactStore for MongoStore {
    async fn list(
        &self,
        company: &CompanyId,
        task_id: Option<&str>,
    ) -> Result<Vec<crate::ports::artifacts::ArtifactRecord>> {
        // `task_id` narrows the query itself rather than filtering after the
        // fetch, so one task's Artifacts tab does not pull the whole company.
        let mut filter = doc! {"company_id": company.as_ref()};
        if let Some(task_id) = task_id {
            filter.insert("task_id", task_id);
        }
        let mut cursor = self
            .collection("artifacts")
            .find(filter)
            .sort(doc! {"updated_ms": -1})
            .await
            .map_err(mongo_err)?;
        let mut out: Vec<crate::ports::artifacts::ArtifactRecord> = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "artifact_json")?)?);
        }
        Ok(out)
    }

    async fn get(
        &self,
        company: &CompanyId,
        id: &str,
    ) -> Result<Option<crate::ports::artifacts::ArtifactRecord>> {
        let found = self
            .collection("artifacts")
            .find_one(doc! {"company_id": company.as_ref(), "artifact_id": id})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(
                &doc,
                "artifact_json",
            )?)?)),
            None => Ok(None),
        }
    }

    async fn upsert(
        &self,
        company: &CompanyId,
        artifact: &crate::ports::artifacts::ArtifactRecord,
    ) -> Result<()> {
        self.collection("artifacts")
            .update_one(
                doc! {"company_id": company.as_ref(), "artifact_id": &artifact.id},
                doc! {"$set": {
                    "task_id": &artifact.task_id,
                    "artifact_json": serde_json::to_string(artifact)?,
                    "updated_ms": artifact.updated_at_millis as i64,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn delete(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let res = self
            .collection("artifacts")
            .delete_one(doc! {"company_id": company.as_ref(), "artifact_id": id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }
}

// ---------------------------------------------------------------------------
// WorkflowRevisionStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::workflow_revisions::WorkflowRevisionStore for MongoStore {
    async fn push_revision(
        &self,
        company: &CompanyId,
        revision: &crate::ports::workflow_revisions::WorkflowRevisionRecord,
    ) -> Result<()> {
        use crate::ports::workflow_revisions::MAX_WORKFLOW_REVISIONS;
        let coll = self.collection("workflow_revisions");
        coll.insert_one(doc! {
            "company_id": company.as_ref(),
            "revision_id": &revision.id,
            "workflow_id": &revision.workflow_id,
            "revision_json": serde_json::to_string(revision)?,
            "created_ms": revision.created_at_millis as i64,
        })
        .await
        .map_err(mongo_err)?;

        // Prune to the cap: collect this workflow's revision ids newest-first and
        // delete everything past `MAX`. Two statements rather than one because
        // MongoDB has no "delete all but the newest N" operator; the compound
        // index keeps the read cheap, and an interleaved second push only ever
        // trims further, never resurrects a pruned row.
        let mut cursor = coll
            .find(doc! {"company_id": company.as_ref(), "workflow_id": &revision.workflow_id})
            .sort(doc! {"created_ms": -1, "revision_id": -1})
            .await
            .map_err(mongo_err)?;
        let mut ids: Vec<String> = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            ids.push(get_str(&doc, "revision_id")?);
        }
        if ids.len() > MAX_WORKFLOW_REVISIONS {
            let stale: Vec<&String> = ids.iter().skip(MAX_WORKFLOW_REVISIONS).collect();
            coll.delete_many(doc! {
                "company_id": company.as_ref(),
                "workflow_id": &revision.workflow_id,
                "revision_id": {"$in": stale},
            })
            .await
            .map_err(mongo_err)?;
        }
        Ok(())
    }

    async fn list_revisions(
        &self,
        company: &CompanyId,
        workflow_id: &str,
    ) -> Result<Vec<crate::ports::workflow_revisions::WorkflowRevisionRecord>> {
        let mut cursor = self
            .collection("workflow_revisions")
            .find(doc! {"company_id": company.as_ref(), "workflow_id": workflow_id})
            .sort(doc! {"created_ms": -1, "revision_id": -1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "revision_json")?)?);
        }
        Ok(out)
    }

    async fn get_revision(
        &self,
        company: &CompanyId,
        workflow_id: &str,
        revision_id: &str,
    ) -> Result<Option<crate::ports::workflow_revisions::WorkflowRevisionRecord>> {
        let found = self
            .collection("workflow_revisions")
            .find_one(doc! {
                "company_id": company.as_ref(),
                "workflow_id": workflow_id,
                "revision_id": revision_id,
            })
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(
                &doc,
                "revision_json",
            )?)?)),
            None => Ok(None),
        }
    }

    async fn delete_revisions(&self, company: &CompanyId, workflow_id: &str) -> Result<u64> {
        let res = self
            .collection("workflow_revisions")
            .delete_many(doc! {"company_id": company.as_ref(), "workflow_id": workflow_id})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count)
    }
}

// ---------------------------------------------------------------------------
// WorkflowRunOutputStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::run_output::WorkflowRunOutputStore for MongoStore {
    async fn put_run_output(
        &self,
        company: &CompanyId,
        record: &crate::ports::run_output::WorkflowRunOutputRecord,
    ) -> Result<()> {
        use crate::ports::run_output::MAX_RUN_OUTPUTS_PER_COMPANY;
        let coll = self.collection("run_outputs");
        // Upsert: last-write-wins per `(company, run_id)`, so a re-run overwrites
        // rather than stacking. The unique index is what makes the filter a key.
        coll.update_one(
            doc! {"company_id": company.as_ref(), "run_id": &record.run_id},
            doc! {"$set": {
                "company_id": company.as_ref(),
                "run_id": &record.run_id,
                "workflow_id": &record.workflow_id,
                "at_ms": record.at_millis as i64,
                "output_json": serde_json::to_string(record)?,
            }},
        )
        .with_options(UpdateOptions::builder().upsert(true).build())
        .await
        .map_err(mongo_err)?;

        // Prune to the cap: collect this company's run ids newest-first and delete
        // everything past `MAX`. Two statements, like the revision prune — Mongo
        // has no "delete all but the newest N" operator; the recency index keeps
        // the read cheap.
        let mut cursor = coll
            .find(doc! {"company_id": company.as_ref()})
            .sort(doc! {"at_ms": -1, "run_id": -1})
            .await
            .map_err(mongo_err)?;
        let mut ids: Vec<String> = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            ids.push(get_str(&doc, "run_id")?);
        }
        if ids.len() > MAX_RUN_OUTPUTS_PER_COMPANY {
            let stale: Vec<&String> = ids.iter().skip(MAX_RUN_OUTPUTS_PER_COMPANY).collect();
            coll.delete_many(doc! {
                "company_id": company.as_ref(),
                "run_id": {"$in": stale},
            })
            .await
            .map_err(mongo_err)?;
        }
        Ok(())
    }

    async fn get_run_output(
        &self,
        company: &CompanyId,
        run_id: &str,
    ) -> Result<Option<crate::ports::run_output::WorkflowRunOutputRecord>> {
        let found = self
            .collection("run_outputs")
            .find_one(doc! {"company_id": company.as_ref(), "run_id": run_id})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "output_json")?)?)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// RunStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::runs::RunStore for MongoStore {
    async fn create_run(
        &self,
        company: &CompanyId,
        spec: crate::ports::runs::NewRun,
    ) -> Result<crate::ports::runs::RunRecord> {
        use crate::ports::runs::{RunRecord, RunStatus};

        // The ordinal comes from the same atomic `$inc` counter the event and
        // usage sequences use, keyed per card — so concurrent creates cannot
        // collide even across processes. `next_seq` is 0-based; attempts are
        // 1-based (`Attempt 1` is the first).
        let attempt = self
            .next_seq(company, &format!("run:{}", spec.task_id))
            .await?
            .saturating_add(1);
        let run = RunRecord {
            id: spec.id,
            company: company.clone(),
            task_id: spec.task_id,
            agent_id: spec.agent_id,
            attempt: attempt as u32,
            status: RunStatus::Pending,
            trigger_event_seq: None,
            created_at_millis: now_millis(),
            started_at_millis: None,
            finished_at_millis: None,
            error: None,
            usage: Default::default(),
            step_count: 0,
        };
        // A plain insert, not an upsert: the unique `(company_id, run_id)`
        // index is what turns a repeated id into the port's documented
        // conflict instead of a silently overwritten attempt.
        let existing = self
            .collection("runs")
            .find_one(doc! {"company_id": company.as_ref(), "run_id": &run.id})
            .await
            .map_err(mongo_err)?;
        if existing.is_some() {
            return Err(OpenCompanyError::Conflict(format!(
                "run '{}' already exists",
                run.id
            )));
        }
        self.collection("runs")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "run_id": &run.id,
                "task_id": &run.task_id,
                "status": run.status.as_str(),
                "attempt": run.attempt as i64,
                "created_ms": run.created_at_millis as i64,
                "run_json": serde_json::to_string(&run)?,
            })
            .await
            .map_err(mongo_err)?;
        Ok(run)
    }

    async fn get_run(
        &self,
        company: &CompanyId,
        id: &str,
    ) -> Result<Option<crate::ports::runs::RunRecord>> {
        let found = self
            .collection("runs")
            .find_one(doc! {"company_id": company.as_ref(), "run_id": id})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(serde_json::from_str(&get_str(&doc, "run_json")?)?)),
            None => Ok(None),
        }
    }

    async fn put_run(
        &self,
        company: &CompanyId,
        run: &crate::ports::runs::RunRecord,
    ) -> Result<()> {
        self.collection("runs")
            .update_one(
                doc! {"company_id": company.as_ref(), "run_id": &run.id},
                doc! {"$set": {
                    "task_id": &run.task_id,
                    "status": run.status.as_str(),
                    "attempt": run.attempt as i64,
                    "created_ms": run.created_at_millis as i64,
                    "run_json": serde_json::to_string(run)?,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn list_runs(
        &self,
        company: &CompanyId,
        filter: &crate::ports::runs::RunFilter,
    ) -> Result<Vec<crate::ports::runs::RunRecord>> {
        let mut query = doc! {"company_id": company.as_ref()};
        if let Some(task_id) = &filter.task_id {
            query.insert("task_id", task_id.as_str());
        }
        if !filter.statuses.is_empty() {
            let statuses: Vec<&str> = filter.statuses.iter().map(|s| s.as_str()).collect();
            query.insert("status", doc! {"$in": statuses});
        }
        // The canonical port ordering (see `runs::sort_newest_first`), pushed
        // into the query so the limit truncates the right end.
        let runs = self.collection("runs");
        let mut find = runs
            .find(query)
            .sort(doc! {"created_ms": -1, "attempt": -1, "run_id": -1});
        if let Some(limit) = filter.limit {
            match find_limit(limit) {
                FindLimit::Empty => return Ok(Vec::new()),
                FindLimit::Unlimited => {}
                FindLimit::AtMost(n) => find = find.limit(n),
            }
        }
        let mut cursor = find.await.map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "run_json")?)?);
        }
        Ok(out)
    }

    async fn append_run_step(
        &self,
        company: &CompanyId,
        step: &crate::ports::runs::RunStepRecord,
    ) -> Result<()> {
        // Upsert on `(run_id, step_seq)`: a replayed append overwrites rather
        // than duplicating, matching the other two backends.
        self.collection("run_steps")
            .update_one(
                doc! {
                    "company_id": company.as_ref(),
                    "run_id": &step.run_id,
                    "step_seq": step.step_seq as i64,
                },
                doc! {"$set": {
                    "at_ms": step.at_millis as i64,
                    "step_json": serde_json::to_string(step)?,
                }},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn list_run_steps(
        &self,
        company: &CompanyId,
        run_id: &str,
    ) -> Result<Vec<crate::ports::runs::RunStepRecord>> {
        let mut cursor = self
            .collection("run_steps")
            .find(doc! {"company_id": company.as_ref(), "run_id": run_id})
            .sort(doc! {"step_seq": 1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "step_json")?)?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// ScheduleFireStore (issue #241)
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::schedule_fires::ScheduleFireStore for MongoStore {
    async fn claim_fire(
        &self,
        company: &CompanyId,
        schedule_id: &str,
        minute: u64,
    ) -> Result<bool> {
        // A plain `insert_one` against the unique `(company_id, schedule_id,
        // scheduled_for)` index. Success means this caller won the race; an
        // `E11000` duplicate-key means a peer — another replica, or this process
        // before a restart — already claimed the instant, so the caller lost and
        // must skip. Every OTHER driver error propagates: a claim store that
        // cannot answer must fail closed at the scheduler, never be read as a win.
        let result = self
            .collection("schedule_fires")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "schedule_id": schedule_id,
                "scheduled_for": minute as i64,
                "claimed_at_ms": now_millis() as i64,
            })
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(e) if is_duplicate_key(&e) => Ok(false),
            Err(e) => Err(mongo_err(e)),
        }
    }

    async fn latest_fire(&self, company: &CompanyId, schedule_id: &str) -> Result<Option<u64>> {
        // The single newest row for this schedule, straight off the compound
        // index — no aggregation needed. A schedule that never fired matches
        // nothing and yields `None`, the no-anchor case.
        let found = self
            .collection("schedule_fires")
            .find_one(doc! {"company_id": company.as_ref(), "schedule_id": schedule_id})
            .sort(doc! {"scheduled_for": -1})
            .await
            .map_err(mongo_err)?;
        match found {
            Some(doc) => Ok(Some(get_i64(&doc, "scheduled_for")? as u64)),
            None => Ok(None),
        }
    }

    async fn prune_fires_before(&self, company: &CompanyId, cutoff_minute: u64) -> Result<usize> {
        let result = self
            .collection("schedule_fires")
            .delete_many(doc! {
                "company_id": company.as_ref(),
                "scheduled_for": {"$lt": cutoff_minute as i64},
            })
            .await
            .map_err(mongo_err)?;
        Ok(result.deleted_count as usize)
    }
}

// ---------------------------------------------------------------------------
// UsageMeter
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::usage::UsageMeter for MongoStore {
    async fn record(
        &self,
        company: &CompanyId,
        sample: &crate::ports::usage::UsageSample,
    ) -> Result<()> {
        let seq = self.next_seq(company, "usage").await?;
        self.collection("usage_samples")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "seq": seq as i64,
                "at_ms": sample.at_millis as i64,
                "sample_json": serde_json::to_string(sample)?,
            })
            .await
            .map_err(mongo_err)?;
        // Retention: drop samples older than the 90-day window, anchored to the
        // newest sample just written.
        let cutoff = crate::ports::usage::retention_cutoff(sample.at_millis);
        self.collection("usage_samples")
            .delete_many(doc! {
                "company_id": company.as_ref(),
                "at_ms": {"$lt": cutoff as i64},
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn query(
        &self,
        company: &CompanyId,
        since_millis: u64,
    ) -> Result<Vec<crate::ports::usage::UsageSample>> {
        let mut cursor = self
            .collection("usage_samples")
            .find(doc! {"company_id": company.as_ref(), "at_ms": {"$gte": since_millis as i64}})
            .sort(doc! {"at_ms": 1, "seq": 1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "sample_json")?)?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// SkillStateStore
// ---------------------------------------------------------------------------

#[async_trait]
impl crate::ports::skills_state::SkillStateStore for MongoStore {
    async fn list(
        &self,
        company: &CompanyId,
    ) -> Result<Vec<crate::ports::skills_state::SkillState>> {
        let mut cursor = self
            .collection("skill_state")
            .find(doc! {"company_id": company.as_ref()})
            .sort(doc! {"slug": 1})
            .await
            .map_err(mongo_err)?;
        let mut out = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            out.push(serde_json::from_str(&get_str(&doc, "state_json")?)?);
        }
        Ok(out)
    }

    async fn set(
        &self,
        company: &CompanyId,
        state: &crate::ports::skills_state::SkillState,
    ) -> Result<()> {
        self.collection("skill_state")
            .update_one(
                doc! {"company_id": company.as_ref(), "slug": &state.slug},
                doc! {"$set": {"state_json": serde_json::to_string(state)?}},
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn remove(&self, company: &CompanyId, slug: &str) -> Result<bool> {
        let res = self
            .collection("skill_state")
            .delete_one(doc! {"company_id": company.as_ref(), "slug": slug})
            .await
            .map_err(mongo_err)?;
        Ok(res.deleted_count > 0)
    }
}

// ---------------------------------------------------------------------------
// WorkspaceStore
// ---------------------------------------------------------------------------

/// The GridFS bucket every workspace payload lives in.
///
/// One bucket for the whole database rather than one per company: GridFS
/// buckets are a pair of collections, and a per-company bucket would mint two
/// collections per tenant in the shared-database mode this backend exists to
/// serve. Isolation is where it is everywhere else in this file — a
/// `company_id` on every document and a filter on every query — see
/// [`MongoStore::blob_filter`].
const BLOB_BUCKET: &str = "workspace_blobs";

impl MongoStore {
    /// The workspace payload bucket.
    fn blobs(&self) -> mongodb::gridfs::GridFsBucket {
        self.db.gridfs_bucket(
            mongodb::options::GridFsBucketOptions::builder()
                .bucket_name(BLOB_BUCKET.to_string())
                .build(),
        )
    }

    /// The tenancy-scoped filter naming exactly one node's payload.
    ///
    /// **Both keys, every time.** A filter on `node_id` alone would be a
    /// cross-tenant read the moment two companies in one database minted the
    /// same node id — and node ids come from a shared generator, so that is a
    /// collision away rather than an attack away. Every read, every delete and
    /// the boot sweep go through this function so there is no call site that
    /// could have forgotten the company half.
    fn blob_filter(company: &CompanyId, node_id: &str) -> Document {
        doc! {
            "metadata.company_id": company.as_ref(),
            "metadata.node_id": node_id,
        }
    }

    /// Uploads `bytes` as this node's payload and returns nothing.
    ///
    /// The blob is written **before** the node document that names it, on both
    /// the create and the replace path. See
    /// [`create_binary`](crate::ports::workspace::WorkspaceStore::create_binary)
    /// on this type for why that direction.
    async fn put_blob(
        &self,
        company: &CompanyId,
        node_id: &str,
        filename: &str,
        bytes: &[u8],
    ) -> Result<()> {
        use futures::io::AsyncWriteExt;
        let mut upload = self
            .blobs()
            .open_upload_stream(filename)
            .metadata(doc! {
                "company_id": company.as_ref(),
                "node_id": node_id,
            })
            .await
            .map_err(mongo_err)?;
        upload
            .write_all(bytes)
            .await
            .map_err(|e| mongo_err(format!("writing a workspace blob failed: {e}")))?;
        // `close` is what commits the final chunk and the files-collection
        // document. Without it the upload is a partial write that no reader can
        // see — so its failure is the write's failure.
        upload
            .close()
            .await
            .map_err(|e| mongo_err(format!("closing a workspace blob failed: {e}")))?;
        Ok(())
    }

    /// Removes every payload registered to `node_id`, except optionally the one
    /// just written.
    ///
    /// `keep` exists for the replace path: the new blob is uploaded before the
    /// node document is updated, so at that moment the filter matches both
    /// generations and the older one is the one to drop.
    async fn drop_blobs(
        &self,
        company: &CompanyId,
        node_id: &str,
        keep: Option<&mongodb::bson::Bson>,
    ) -> Result<()> {
        let bucket = self.blobs();
        let mut cursor = bucket
            .find(Self::blob_filter(company, node_id))
            .await
            .map_err(mongo_err)?;
        while let Some(file) = cursor.try_next().await.map_err(mongo_err)? {
            if keep == Some(&file.id) {
                continue;
            }
            bucket.delete(file.id).await.map_err(mongo_err)?;
        }
        Ok(())
    }

    /// Deletes blobs whose node document is gone (issue #553).
    ///
    /// The crash window this closes is the one the write ordering deliberately
    /// leaves open: a payload uploaded, then the process dies before the node
    /// document lands. That leaves a blob nothing references — invisible to
    /// every reader, but occupying the tenant's quota forever. The opposite
    /// ordering would have left a node whose download 404s, which is worse, so
    /// this sweep is the price of choosing the survivable direction.
    ///
    /// Runs at construction, where it is cheap (one scan of the files
    /// collection against one projection of the node ids) and where a partial
    /// failure is safe: a sweep that cannot complete is logged and the store is
    /// returned anyway, because refusing to open a company's storage over
    /// reclaimable disk would trade an invisible cost for a total outage.
    async fn sweep_orphan_blobs(&self) -> Result<u64> {
        self.sweep_orphan_blobs_before(SystemTime::now() - INFLIGHT_BLOB_GRACE)
            .await
    }

    /// [`sweep_orphan_blobs`](Self::sweep_orphan_blobs) with the cutoff supplied
    /// rather than derived from the clock.
    ///
    /// Split out so a test can state which side of the grace a blob is on
    /// instead of sleeping for an hour, and so the production path has exactly
    /// one place the grace is applied.
    async fn sweep_orphan_blobs_before(&self, cutoff: SystemTime) -> Result<u64> {
        let live: std::collections::HashSet<(String, String)> = {
            let mut cursor = self
                .collection("workspace_nodes")
                .find(doc! {})
                .await
                .map_err(mongo_err)?;
            let mut out = std::collections::HashSet::new();
            while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
                if let (Ok(company), Ok(node)) = (doc.get_str("company_id"), doc.get_str("node_id"))
                {
                    out.insert((company.to_string(), node.to_string()));
                }
            }
            out
        };
        let bucket = self.blobs();
        let mut cursor = bucket.find(doc! {}).await.map_err(mongo_err)?;
        let mut removed = 0;
        // Issue #664: a blob younger than this is NOT swept, however orphaned it
        // looks.
        //
        // `create_binary` uploads the blob and inserts the node as two
        // statements, so between them a perfectly healthy upload IS an orphan by
        // this scan's definition. The scan is unfiltered by company — correctly,
        // since it must reclaim for every company in the database — so any
        // process booting here would delete an in-flight upload belonging to any
        // company, including one being written by a different process against
        // the same database (a rolling deploy, an overlapping wake-on-request
        // restart, shared-single-DB tenancy). The victim keeps a node whose blob
        // 404s forever while its recorded `size` still counts against the quota.
        //
        // A `company_id` filter does not fix this and was the tempting wrong
        // answer: the store serves every company in its database and holds no
        // tenant identity, and the same-process race survives any such filter.
        // The race is about TIME, so the guard is about time.
        //
        // Erring is one-directional on purpose. Skipping a genuine orphan costs
        // disk until the next boot sweeps it; deleting a live upload destroys a
        // deliverable the operator will never get back. An hour is far longer
        // than the gap between two adjacent statements and far shorter than the
        // interval between boots that matters.
        while let Some(file) = cursor.try_next().await.map_err(mongo_err)? {
            if file.upload_date.to_system_time() > cutoff {
                continue;
            }
            let key = file.metadata.as_ref().and_then(|m| {
                Some((
                    m.get_str("company_id").ok()?.to_string(),
                    m.get_str("node_id").ok()?.to_string(),
                ))
            });
            // A blob with no usable metadata cannot be matched to a node and is
            // therefore an orphan by definition — it predates this scheme or was
            // written by something that is not this store.
            let orphan = key.map(|k| !live.contains(&k)).unwrap_or(true);
            if orphan {
                bucket.delete(file.id).await.map_err(mongo_err)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Loads every workspace node for a company into an id-keyed map.
    async fn workspace_nodes(
        &self,
        company: &CompanyId,
    ) -> Result<HashMap<String, crate::ports::workspace::WorkspaceNode>> {
        let mut cursor = self
            .collection("workspace_nodes")
            .find(doc! {"company_id": company.as_ref()})
            .await
            .map_err(mongo_err)?;
        let mut out = HashMap::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            let node: crate::ports::workspace::WorkspaceNode =
                serde_json::from_str(&get_str(&doc, "node_json")?)?;
            out.insert(node.id.clone(), node);
        }
        Ok(out)
    }
}

#[async_trait]
impl crate::ports::workspace::WorkspaceStore for MongoStore {
    async fn tree(
        &self,
        company: &CompanyId,
    ) -> Result<Vec<crate::ports::workspace::WorkspaceNode>> {
        Ok(self.workspace_nodes(company).await?.into_values().collect())
    }

    async fn read(
        &self,
        company: &CompanyId,
        id: &str,
    ) -> Result<Option<(crate::ports::workspace::WorkspaceNode, String)>> {
        let doc = self
            .collection("workspace_nodes")
            .find_one(doc! {"company_id": company.as_ref(), "node_id": id})
            .await
            .map_err(mongo_err)?;
        match doc {
            Some(doc) => {
                let node: crate::ports::workspace::WorkspaceNode =
                    serde_json::from_str(&get_str(&doc, "node_json")?)?;
                // A binary node reads as an empty body, like a folder — the port
                // contract that keeps every prose-shaped caller correct.
                let content = if node.is_binary() {
                    String::new()
                } else {
                    get_str(&doc, "content")?
                };
                Ok(Some((node, content)))
            }
            None => Ok(None),
        }
    }

    async fn write(
        &self,
        company: &CompanyId,
        id: &str,
        content: &str,
        author: crate::ports::workspace::WorkspaceOrigin,
    ) -> Result<crate::ports::workspace::WorkspaceNode> {
        use crate::ports::workspace::NodeKind;
        let doc = self
            .collection("workspace_nodes")
            .find_one(doc! {"company_id": company.as_ref(), "node_id": id})
            .await
            .map_err(mongo_err)?;
        let Some(doc) = doc else {
            return Err(OpenCompanyError::CompanyNotFound(format!(
                "workspace node {id}"
            )));
        };
        let mut node: crate::ports::workspace::WorkspaceNode =
            serde_json::from_str(&get_str(&doc, "node_json")?)?;
        if node.kind != NodeKind::File {
            return Err(OpenCompanyError::InvalidRequest(
                "cannot write content to a folder".to_string(),
            ));
        }
        if let Some(mime) = node.mime.clone() {
            return Err(OpenCompanyError::InvalidRequest(
                crate::ports::workspace::binary_write_refusal(&node.name, &mime),
            ));
        }
        node.updated_at_millis = now_millis();
        // Authorship rides the same stamp as the timestamp. The node is stored
        // as opaque JSON in `node_json`, so this needs no schema change.
        node.updated_by = author;
        self.collection("workspace_nodes")
            .update_one(
                doc! {"company_id": company.as_ref(), "node_id": id},
                doc! {"$set": {
                    "node_json": serde_json::to_string(&node)?,
                    "content": content,
                    "updated_ms": node.updated_at_millis as i64,
                }},
            )
            .await
            .map_err(mongo_err)?;
        Ok(node)
    }

    async fn create(
        &self,
        company: &CompanyId,
        node: &crate::ports::workspace::WorkspaceNode,
        content: Option<&str>,
    ) -> Result<()> {
        use crate::ports::workspace::NodeKind;
        let nodes = self.workspace_nodes(company).await?;
        if nodes.contains_key(&node.id) {
            return Err(OpenCompanyError::Conflict(format!(
                "workspace node {} already exists",
                node.id
            )));
        }
        if let Some(parent) = &node.parent_id {
            match nodes.get(parent) {
                Some(p) if p.kind == NodeKind::Folder => {}
                Some(_) => {
                    return Err(OpenCompanyError::InvalidRequest(
                        "parent is not a folder".to_string(),
                    ));
                }
                None => {
                    return Err(OpenCompanyError::InvalidRequest(
                        "parent folder does not exist".to_string(),
                    ));
                }
            }
        }
        self.collection("workspace_nodes")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "node_id": &node.id,
                "node_json": serde_json::to_string(node)?,
                "content": content.unwrap_or(""),
                "updated_ms": node.updated_at_millis as i64,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    /// GridFS — the only backend where the payload cannot ride in the record.
    ///
    /// A BSON document caps at 16 MB, and the artifacts this issue exists for
    /// are generated images and video that routinely exceed it, so the bytes go
    /// into a bucket and the node document keeps only the metadata.
    ///
    /// # Blob first, document second
    ///
    /// The two writes cannot be made atomic without a transaction this backend
    /// does not require of its deployment, so the ordering is chosen by which
    /// failure is survivable. A crash between them leaves a blob with no node:
    /// invisible to every reader, costing disk until
    /// [`sweep_orphan_blobs`](MongoStore::sweep_orphan_blobs) runs at the next
    /// boot. The reverse would leave a node the tree shows and the download
    /// 404s on — a file the operator can see and cannot fetch, with nothing to
    /// repair it. Deletion runs the mirror image (document first, blob second)
    /// for the same reason.
    async fn create_binary(
        &self,
        company: &CompanyId,
        node: &crate::ports::workspace::WorkspaceNode,
        bytes: &[u8],
    ) -> Result<()> {
        use crate::ports::workspace::NodeKind;
        let node = crate::ports::workspace::stamped_binary(node, bytes)?;
        let nodes = self.workspace_nodes(company).await?;
        if nodes.contains_key(&node.id) {
            return Err(OpenCompanyError::Conflict(format!(
                "workspace node {} already exists",
                node.id
            )));
        }
        if let Some(parent) = &node.parent_id {
            match nodes.get(parent) {
                Some(p) if p.kind == NodeKind::Folder => {}
                Some(_) => {
                    return Err(OpenCompanyError::InvalidRequest(
                        "parent is not a folder".to_string(),
                    ));
                }
                None => {
                    return Err(OpenCompanyError::InvalidRequest(
                        "parent folder does not exist".to_string(),
                    ));
                }
            }
        }
        self.put_blob(company, &node.id, &node.name, bytes).await?;
        self.collection("workspace_nodes")
            .insert_one(doc! {
                "company_id": company.as_ref(),
                "node_id": &node.id,
                "node_json": serde_json::to_string(&node)?,
                "content": "",
                "updated_ms": node.updated_at_millis as i64,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn write_binary(
        &self,
        company: &CompanyId,
        id: &str,
        bytes: &[u8],
        mime: Option<&str>,
        author: crate::ports::workspace::WorkspaceOrigin,
    ) -> Result<crate::ports::workspace::WorkspaceNode> {
        let doc = self
            .collection("workspace_nodes")
            .find_one(doc! {"company_id": company.as_ref(), "node_id": id})
            .await
            .map_err(mongo_err)?;
        let Some(doc) = doc else {
            return Err(OpenCompanyError::CompanyNotFound(format!(
                "workspace node {id}"
            )));
        };
        let mut node: crate::ports::workspace::WorkspaceNode =
            serde_json::from_str(&get_str(&doc, "node_json")?)?;
        crate::ports::workspace::rebind_binary(&mut node, bytes, mime, author)?;
        // New blob, then the document, then the old blob — the same "a reader
        // never sees a node without its bytes" ordering as the create path. The
        // worst crash outcome is a superseded blob left behind for the boot
        // sweep.
        let superseded: Vec<mongodb::bson::Bson> = {
            let mut cursor = self
                .blobs()
                .find(Self::blob_filter(company, id))
                .await
                .map_err(mongo_err)?;
            let mut ids = Vec::new();
            while let Some(file) = cursor.try_next().await.map_err(mongo_err)? {
                ids.push(file.id);
            }
            ids
        };
        self.put_blob(company, id, &node.name, bytes).await?;
        self.collection("workspace_nodes")
            .update_one(
                doc! {"company_id": company.as_ref(), "node_id": id},
                doc! {"$set": {
                    "node_json": serde_json::to_string(&node)?,
                    "content": "",
                    "updated_ms": node.updated_at_millis as i64,
                }},
            )
            .await
            .map_err(mongo_err)?;
        let bucket = self.blobs();
        for old in superseded {
            bucket.delete(old).await.map_err(mongo_err)?;
        }
        Ok(node)
    }

    /// Streams straight out of GridFS — a video is never resident, which is the
    /// reason this backend needed a bucket rather than a bigger field.
    async fn read_bytes(
        &self,
        company: &CompanyId,
        id: &str,
    ) -> Result<
        Option<(
            crate::ports::workspace::WorkspaceNode,
            crate::ports::workspace::BlobStream,
        )>,
    > {
        let Some(doc) = self
            .collection("workspace_nodes")
            .find_one(doc! {"company_id": company.as_ref(), "node_id": id})
            .await
            .map_err(mongo_err)?
        else {
            return Ok(None);
        };
        let node: crate::ports::workspace::WorkspaceNode =
            serde_json::from_str(&get_str(&doc, "node_json")?)?;
        if !node.is_binary() {
            return Ok(None);
        }
        let bucket = self.blobs();
        // Located by the tenancy-scoped metadata filter rather than by an id
        // carried on the node document: the bucket is shared across companies,
        // so the company half of the filter is the isolation boundary and must
        // be applied by the query that finds the file, not checked afterwards.
        let Some(file) = bucket
            .find_one(Self::blob_filter(company, id))
            .await
            .map_err(mongo_err)?
        else {
            return Ok(None);
        };
        let stream = bucket
            .open_download_stream(file.id)
            .await
            .map_err(mongo_err)?;
        use tokio_util::compat::FuturesAsyncReadCompatExt;
        let reader = tokio_util::io::ReaderStream::new(stream.compat());
        Ok(Some((
            node,
            Box::pin(futures::StreamExt::map(reader, |chunk| {
                chunk.map_err(|e| {
                    OpenCompanyError::Store(format!("reading a workspace blob failed: {e}"))
                })
            })),
        )))
    }

    async fn rename_move(
        &self,
        company: &CompanyId,
        id: &str,
        name: Option<&str>,
        parent: Option<Option<&str>>,
    ) -> Result<crate::ports::workspace::WorkspaceNode> {
        use crate::ports::workspace::NodeKind;
        let nodes = self.workspace_nodes(company).await?;
        if !nodes.contains_key(id) {
            return Err(OpenCompanyError::CompanyNotFound(format!(
                "workspace node {id}"
            )));
        }
        // A move to root (`Some(None)`) never forms a cycle.
        if let Some(Some(parent)) = parent {
            if parent == id || mongo_workspace_descendants(&nodes, id).contains(parent) {
                return Err(OpenCompanyError::InvalidRequest(
                    "cannot move a folder into its own subtree".to_string(),
                ));
            }
            if nodes.get(parent).map(|p| p.kind) != Some(NodeKind::Folder) {
                return Err(OpenCompanyError::InvalidRequest(
                    "target parent is not a folder".to_string(),
                ));
            }
        }
        let mut node = nodes.get(id).cloned().expect("node present");
        if let Some(name) = name {
            node.name = name.to_string();
        }
        if let Some(parent) = parent {
            node.parent_id = parent.map(str::to_string);
        }
        node.updated_at_millis = now_millis();
        self.collection("workspace_nodes")
            .update_one(
                doc! {"company_id": company.as_ref(), "node_id": id},
                doc! {"$set": {
                    "node_json": serde_json::to_string(&node)?,
                    "updated_ms": node.updated_at_millis as i64,
                }},
            )
            .await
            .map_err(mongo_err)?;
        Ok(node)
    }

    async fn delete(&self, company: &CompanyId, id: &str) -> Result<bool> {
        let nodes = self.workspace_nodes(company).await?;
        if !nodes.contains_key(id) {
            return Ok(false);
        }
        let mut to_remove = mongo_workspace_descendants(&nodes, id);
        to_remove.insert(id.to_string());
        let ids: Vec<&String> = to_remove.iter().collect();
        // Documents first, blobs second — the mirror of the write ordering. A
        // crash between them leaves a payload with no node, which the boot
        // sweep reclaims; the reverse would leave a node whose download 404s.
        self.collection("workspace_nodes")
            .delete_many(doc! {"company_id": company.as_ref(), "node_id": {"$in": &ids}})
            .await
            .map_err(mongo_err)?;
        for node_id in &to_remove {
            self.drop_blobs(company, node_id, None).await?;
        }
        Ok(true)
    }

    async fn is_empty(&self, company: &CompanyId) -> Result<bool> {
        let count = self
            .collection("workspace_nodes")
            .count_documents(doc! {"company_id": company.as_ref()})
            .await
            .map_err(mongo_err)?;
        Ok(count == 0)
    }
}

/// Collects the ids of every descendant of `id` (excluding `id`).
fn mongo_workspace_descendants(
    nodes: &HashMap<String, crate::ports::workspace::WorkspaceNode>,
    id: &str,
) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    let mut frontier = vec![id.to_string()];
    while let Some(current) = frontier.pop() {
        for (child_id, node) in nodes {
            if node.parent_id.as_deref() == Some(current.as_str()) && out.insert(child_id.clone()) {
                frontier.push(child_id.clone());
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests — env-gated conformance against a real MongoDB
// ---------------------------------------------------------------------------

/// The conformance suite needs a live server; there is no in-process MongoDB.
/// Set `OPENCOMPANY_TEST_MONGODB_URI` (e.g. `mongodb://localhost:27017`) to
/// run these; without it every test is a skip, keeping `cargo test` offline.
///
/// CI additionally sets `OPENCOMPANY_TEST_MONGODB_REQUIRED=1`, which turns that
/// skip into a failure — see `required()` below.
#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::store::conformance;

    static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Whether a missing server must FAIL rather than skip. Issue #555.
    ///
    /// The `OPENCOMPANY_TEST_MONGODB_URI` skip above is right for a laptop with
    /// no MongoDB — it keeps a default `cargo test` offline — and wrong for the
    /// CI lane whose entire purpose is running this suite. There, an unset URI
    /// is a misconfigured job, and the skip would report it as a pass: the
    /// whole suite silently absent behind a green tick, which is the exact
    /// defect this lane was added to fix, reintroduced one layer down.
    ///
    /// So CI sets this second variable and nothing else does. Set = the caller
    /// has promised a reachable server, so not finding one is an error.
    ///
    /// `0` and the empty string read as unset, so the variable can be threaded
    /// through a workflow matrix or a shell wrapper that always defines it.
    fn required() -> bool {
        std::env::var("OPENCOMPANY_TEST_MONGODB_REQUIRED")
            .is_ok_and(|value| !value.is_empty() && value != "0")
    }

    /// The URI with any `user:password@` replaced by `***@`, for the panic
    /// message below.
    ///
    /// The unreachable-server panic names the URI so the failure says *which*
    /// server it could not reach — a bare "connection refused" in a CI log is
    /// most of a debugging session. But a connection string carries its
    /// credentials inline, and a panic lands in the CI log, the terminal
    /// scrollback and any artifact that captures either. CI points at an
    /// unauthenticated localhost, so nothing leaks there; a developer pointing
    /// this suite at a real cluster is the case that would, and that is exactly
    /// when the message is most useful. Redacting keeps the host and port,
    /// which is the part worth printing.
    fn redact_credentials(uri: &str) -> String {
        let Some((scheme, rest)) = uri.split_once("://") else {
            return uri.to_string();
        };
        // Userinfo, when present, precedes the first `/` of the path — so only
        // an `@` before that boundary delimits it. A password may itself
        // contain `@`, so split at the LAST one within the authority.
        let authority_end = rest.find('/').unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(authority_end);
        match authority.rfind('@') {
            Some(at) => format!("{scheme}://***{}{tail}", &authority[at..]),
            None => uri.to_string(),
        }
    }

    #[test]
    fn redaction_keeps_the_host_and_drops_the_credentials() {
        // The CI shape: nothing to redact, nothing changed.
        assert_eq!(
            redact_credentials("mongodb://localhost:27017"),
            "mongodb://localhost:27017"
        );
        // The shape that would leak.
        assert_eq!(
            redact_credentials("mongodb://user:hunter2@cluster.example:27017"),
            "mongodb://***@cluster.example:27017"
        );
        // A password containing `@` — splitting at the FIRST one would leave
        // the tail of the password in the message.
        assert_eq!(
            redact_credentials("mongodb://user:p@ss@cluster.example:27017"),
            "mongodb://***@cluster.example:27017"
        );
        // An `@` in the path or query must not be mistaken for userinfo.
        assert_eq!(
            redact_credentials("mongodb://localhost:27017/db?replicaSet=a@b"),
            "mongodb://localhost:27017/db?replicaSet=a@b"
        );
        // A credentialed URI that also carries a path keeps the path.
        assert_eq!(
            redact_credentials("mongodb+srv://u:p@host/admin?retryWrites=true"),
            "mongodb+srv://***@host/admin?retryWrites=true"
        );
        // Not a URI at all: returned untouched rather than mangled.
        assert_eq!(redact_credentials("localhost:27017"), "localhost:27017");
    }

    async fn store() -> Option<Arc<MongoStore>> {
        let uri = match std::env::var("OPENCOMPANY_TEST_MONGODB_URI") {
            Ok(uri) => uri,
            Err(_) => {
                assert!(
                    !required(),
                    "OPENCOMPANY_TEST_MONGODB_REQUIRED is set but \
                     OPENCOMPANY_TEST_MONGODB_URI is not. This lane exists to run the \
                     MongoDB conformance suite against a real server, so a skip here is \
                     a misconfigured job rather than a pass — point the URI at the \
                     service container."
                );
                eprintln!("skipping: OPENCOMPANY_TEST_MONGODB_URI is not set");
                return None;
            }
        };
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let db = format!(
            "oc_test_{}_{}_{}",
            std::process::id(),
            nonce,
            DB_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        // `connect` creates indexes, so it round-trips to the server rather
        // than resolving lazily: an unreachable host fails HERE, after the
        // driver's server-selection timeout, instead of much later inside
        // whichever assertion happened to touch the database first.
        let store = MongoStore::connect(&uri, &db).await.unwrap_or_else(|err| {
            panic!(
                "could not reach the MongoDB server at {}: {err}",
                redact_credentials(&uri)
            )
        });
        Some(Arc::new(store))
    }

    async fn drop_db(store: &MongoStore) {
        let _ = store.db.drop().await;
    }

    /// The boot sweep reclaims a payload whose node document never landed.
    ///
    /// This is the crash the write ordering deliberately allows: blob first,
    /// document second, so an interrupted `create_binary` leaves bytes nothing
    /// references. Seeded here directly — uploading to the bucket without ever
    /// inserting the node — because that is precisely the state a crash between
    /// the two writes produces, and it is not reachable through the port.
    ///
    /// The node-backed blob beside it is the half that must be left alone: a
    /// sweep that reclaimed live payloads would be far worse than the leak it
    /// fixes.
    #[tokio::test]
    async fn the_boot_sweep_reclaims_orphaned_blobs_and_spares_live_ones() {
        let Some(s) = store().await else { return };
        let company = CompanyId::new("sweep-co");

        // A live binary node, written through the port.
        let node = crate::ports::workspace::WorkspaceNode {
            id: "keep".to_string(),
            name: "keep.png".to_string(),
            kind: crate::ports::workspace::NodeKind::File,
            parent_id: None,
            updated_at_millis: now_millis(),
            created_by: crate::ports::workspace::WorkspaceOrigin::Operator,
            updated_by: crate::ports::workspace::WorkspaceOrigin::Operator,
            mime: Some("image/png".to_string()),
            size: None,
            sha256: None,
        };
        crate::ports::workspace::WorkspaceStore::create_binary(&*s, &company, &node, b"live-bytes")
            .await
            .unwrap();

        // …and a dangling one: the bytes of an interrupted create.
        s.put_blob(&company, "vanished", "ghost.png", b"orphan-bytes")
            .await
            .unwrap();
        // A blob with no metadata at all — a shape this store never writes, and
        // therefore unmatchable to any node, so it is an orphan by definition.
        {
            use futures::io::AsyncWriteExt;
            let mut up = s
                .blobs()
                .open_upload_stream("nometa.bin")
                .await
                .expect("upload");
            up.write_all(b"no-metadata").await.unwrap();
            up.close().await.unwrap();
        }

        let before = s.blobs().find(doc! {}).await.unwrap();
        assert_eq!(
            before.try_collect::<Vec<_>>().await.unwrap().len(),
            3,
            "two orphans and one live payload are staged"
        );

        // Age every blob past the in-flight grace (issue #664). Without this the
        // sweep spares all three — correctly, since a blob seconds old may be a
        // write in progress — and this test would assert nothing. Backdating is
        // what makes it a test about *orphans* rather than about the clock.
        backdate_blobs(&s).await;

        // Constructing a store over the same database runs the sweep.
        let rebooted = MongoStore::from_database(s.db.clone()).await.unwrap();

        let files = rebooted
            .blobs()
            .find(doc! {})
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(files.len(), 1, "both orphans are reclaimed");
        assert_eq!(files[0].filename.as_deref(), Some("keep.png"));

        // The live node still serves its bytes — the sweep did not touch it.
        let (_, stream) =
            crate::ports::workspace::WorkspaceStore::read_bytes(&rebooted, &company, "keep")
                .await
                .unwrap()
                .expect("the live payload survives the sweep");
        let mut got = Vec::new();
        {
            use futures::StreamExt;
            let mut stream = stream;
            while let Some(chunk) = stream.next().await {
                got.extend_from_slice(&chunk.unwrap());
            }
        }
        assert_eq!(got, b"live-bytes".to_vec());

        drop_db(&s).await;
    }

    /// Moves every blob's `uploadDate` an hour and a half into the past, so the
    /// in-flight grace (issue #664) no longer covers it.
    ///
    /// Reaches into the GridFS files collection directly because the bucket API
    /// offers no way to age an upload, and the alternative — sleeping past the
    /// grace — would put an hour into the suite.
    async fn backdate_blobs(store: &MongoStore) {
        let old = mongodb::bson::DateTime::from_system_time(
            SystemTime::now() - Duration::from_secs(90 * 60),
        );
        store
            .db
            .collection::<Document>(&format!("{BLOB_BUCKET}.files"))
            .update_many(doc! {}, doc! {"$set": {"uploadDate": old}})
            .await
            .expect("backdating the staged blobs");
    }

    /// Issue #664. A blob uploaded moments ago is NOT reclaimed, however
    /// orphaned it looks.
    ///
    /// This is the state `create_binary` passes through on every binary write:
    /// the payload is uploaded, and for the width of one insert there is no node
    /// pointing at it. A sweep running in that window — a rolling deploy, an
    /// overlapping wake-on-request restart, another tenant booting against a
    /// shared database — used to delete it, leaving a node whose download 404s
    /// forever while its recorded size still counted against the quota.
    ///
    /// The sweep is deliberately not filtered by company, because it must
    /// reclaim for every company in the database. That is exactly why the guard
    /// has to be about time rather than about ownership.
    #[tokio::test]
    async fn the_boot_sweep_leaves_an_in_flight_upload_alone() {
        let Some(s) = store().await else { return };
        let company = CompanyId::new("inflight-co");

        // Precisely the intermediate state of `create_binary`: bytes uploaded,
        // node document not yet inserted.
        s.put_blob(&company, "in-flight", "uploading.png", b"half-written")
            .await
            .unwrap();

        // A second process boots against the same database and sweeps.
        let rebooted = MongoStore::from_database(s.db.clone()).await.unwrap();

        let files = rebooted
            .blobs()
            .find(doc! {})
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            files.len(),
            1,
            "an upload younger than the grace must survive a concurrent boot sweep"
        );
        assert_eq!(files[0].filename.as_deref(), Some("uploading.png"));

        // And the control: once it is genuinely old, it IS reclaimed — so the
        // guard delays reclamation rather than abandoning it, and this test
        // cannot pass against a sweep that simply stopped deleting.
        backdate_blobs(&s).await;
        let removed = rebooted
            .sweep_orphan_blobs()
            .await
            .expect("the sweep runs again");
        assert_eq!(removed, 1, "an aged orphan is still reclaimed");

        drop_db(&s).await;
    }

    /// Shared-single-DB namespacing: two tenants registering the same template
    /// name land distinct namespaced ids in one database, so the `companies`
    /// unique index never conflicts, and the `owners` rows carry the right
    /// tenant for each. Mirrors what the workload does when
    /// `OPENCOMPANY_TENANT_ID` is set (see `AppConfig::namespaced_company_id`).
    #[tokio::test]
    async fn shared_db_namespaced_companies_do_not_conflict() {
        let Some(s) = store().await else { return };

        let manifest: CompanyManifest = toml::from_str("[company]\nname = \"Acme\"\n").unwrap();
        let id_a = crate::app::namespace_company_id(
            "tenant-a",
            crate::runtime::company_id_from_name(&manifest.company.name),
        );
        let id_b = crate::app::namespace_company_id(
            "tenant-b",
            crate::runtime::company_id_from_name(&manifest.company.name),
        );
        assert_eq!(id_a.as_ref(), "tenant-a--acme");
        assert_eq!(id_b.as_ref(), "tenant-b--acme");

        for (id, tenant) in [(&id_a, "tenant-a"), (&id_b, "tenant-b")] {
            let record = CompanyRecord {
                id: id.clone(),
                manifest: manifest.clone(),
                ledger: Vec::new(),
                lifecycle: "running".into(),
                overlay_agents: Vec::new(),
                overlay_desk_members: Vec::new(),
                overlay_desk_order: Vec::new(),
                overlay_desks: Vec::new(),
                overlay_workflows: Vec::new(),
                overlay_budgets: Vec::new(),
                disabled_workflows: Vec::new(),
                template_provenance: None,
            };
            // Same template name under two tenants: distinct namespaced ids, no
            // `companies` unique-index conflict.
            s.save(&record).await.expect("save namespaced company");
            s.set_owner(id, tenant).await.expect("record owner");
        }

        let mut owners = s.owners().await.unwrap();
        owners.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        assert_eq!(
            owners,
            vec![
                (id_a.clone(), "tenant-a".to_string()),
                (id_b.clone(), "tenant-b".to_string()),
            ]
        );

        // Both companies remain addressable and carry the shared template name.
        assert_eq!(
            s.load(&id_a).await.unwrap().unwrap().manifest.company.name,
            "Acme"
        );
        assert_eq!(
            s.load(&id_b).await.unwrap().unwrap().manifest.company.name,
            "Acme"
        );

        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_isolation_by_company() {
        let Some(s) = store().await else { return };
        conformance::assert_isolation_by_company(s.clone(), s.clone(), s.clone(), s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_user_store() {
        let Some(s) = store().await else { return };
        conformance::assert_user_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_session_store() {
        let Some(s) = store().await else { return };
        conformance::assert_session_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_login_code_store() {
        let Some(s) = store().await else { return };
        conformance::assert_login_code_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_append_only_event_and_ledger() {
        let Some(s) = store().await else { return };
        conformance::assert_append_only_event_and_ledger(s.clone(), s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_monotonic_event_seq() {
        let Some(s) = store().await else { return };
        conformance::assert_monotonic_event_seq(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_event_retention() {
        let Some(s) = store().await else { return };
        conformance::assert_event_retention(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_export_totality() {
        let Some(s) = store().await else { return };
        conformance::assert_export_totality(s.clone(), s.clone(), s.clone(), s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_inbox_store() {
        let Some(s) = store().await else { return };
        conformance::assert_inbox_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_task_store() {
        let Some(s) = store().await else { return };
        conformance::assert_task_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_fact_store() {
        let Some(s) = store().await else { return };
        conformance::assert_fact_store(s.clone()).await;
        conformance::assert_artifact_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_context_chunk_stamps() {
        let Some(s) = store().await else { return };
        conformance::assert_context_chunk_stamps(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_run_store() {
        let Some(s) = store().await else { return };
        conformance::assert_run_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_workflow_revision_store() {
        let Some(s) = store().await else { return };
        conformance::assert_workflow_revision_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_run_reaper() {
        let Some(s) = store().await else { return };
        conformance::assert_run_reaper(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_schedule_fire_store() {
        let Some(s) = store().await else { return };
        conformance::assert_schedule_fire_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_usage_meter() {
        let Some(s) = store().await else { return };
        conformance::assert_usage_meter(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_usage_retention() {
        let Some(s) = store().await else { return };
        conformance::assert_usage_retention(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_skill_state_store() {
        let Some(s) = store().await else { return };
        conformance::assert_skill_state_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn conformance_workspace_store() {
        let Some(s) = store().await else { return };
        conformance::assert_workspace_store(s.clone()).await;
        drop_db(&s).await;
    }

    /// The reason issue #553 could not defer the Mongo backend: hosted tenants
    /// run MongoDB, and this is the only lane where the GridFS path — and the
    /// 17 MiB case that proves the 16 MB BSON document cap is not in play —
    /// actually executes.
    #[tokio::test]
    async fn conformance_workspace_binary_store() {
        let Some(s) = store().await else { return };
        conformance::assert_workspace_binary_store(s.clone()).await;
        drop_db(&s).await;
    }

    #[tokio::test]
    async fn durable_ownership_round_trip() {
        let Some(s) = store().await else { return };
        let id = CompanyId::new("acme");
        s.set_owner(&id, "tenant-a").await.expect("set owner");
        s.set_owner(&id, "tenant-b").await.expect("update owner");
        let owners = s.owners().await.expect("owners");
        assert_eq!(owners, vec![(id.clone(), "tenant-b".to_string())]);
        s.remove_owner(&id).await.expect("remove owner");
        assert!(s.owners().await.expect("owners").is_empty());
        drop_db(&s).await;
    }
}
