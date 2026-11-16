// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Filling what a short NZB leaves out from the NZB its own post carries.
//
// A post whose NZB skips past what its recovery rebuilds is given up as its
// articles are built (see `post_health`). Before that, if the post carries
// an NZB of its own, a file named `*.nzb` its NZB lists whole, that file
// alone is fetched, read, and matched against the job's files
// (`weaver_nzb::fill_from`). Where it brings the job back within its
// recovery, its articles for the numbers the job's NZB skips are added to
// the job's files, and the job downloads as any other. Nothing the job's NZB
// lists is replaced, and the job keeps its name, category, and place.
//
// All of it runs in the job's activation, off the pipeline thread, while the
// job is blocked as activating. The job's stored NZB and outline are
// rewritten together, before the lists are handed back, so a restart, a
// pause, or a retry reads the filled NZB.
//
// The setting `inner_nzb_fill` says whether to fill without asking, to ask
// first, or never. Asked, the job is offered and paused; the answer is kept
// in its metadata, and resuming it reads the answer at its next activation.
// A job's own NZB that would not bring it back within its recovery is never
// offered: the job is given up as before, the reason in its error.

use std::sync::Arc;

use weaver_nntp::{NntpClient, ServerSet};
use weaver_nzb::Nzb;

use crate::jobs::ids::JobId;
use crate::jobs::model::{JobSpec, SegmentSpec};
use crate::pipeline::Pipeline;

// The job attribute the fill's state is kept in.
pub const INNER_NZB_FILL_ATTRIBUTE_KEY: &str = "__weaver_inner_nzb_fill";

// The setting that holds the mode. It is read where it is used, off the
// pipeline thread, as a job activates.
pub const INNER_NZB_FILL_SETTING: &str = "downloads.inner_nzb_fill";

// The mode the settings hold; the default when none is stored or it cannot be read.
pub fn stored_mode(db: &crate::Database) -> InnerNzbFillMode {
    db.get_setting(INNER_NZB_FILL_SETTING)
        .ok()
        .flatten()
        .map(|value| InnerNzbFillMode::from_str_or_default(&value))
        .unwrap_or_default()
}

// Whether a job short past its recovery is filled from its post's own NZB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InnerNzbFillMode {
    // Offer the fill and wait for the user.
    #[default]
    Ask,
    // Fill without asking.
    Auto,
    // Never look.
    Off,
}

impl InnerNzbFillMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Auto => "auto",
            Self::Off => "off",
        }
    }

    pub fn from_str_or_default(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "off" => Self::Off,
            _ => Self::Ask,
        }
    }
}

// Where a job stands with its post's own NZB, as its metadata says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillState {
    // Not looked at.
    Untried,
    // Offered: the own NZB fills `filled` of the `skipped` articles the job's
    // NZB leaves out, leaving `left`. The job is paused for an answer.
    Offered {
        filled: u64,
        skipped: u64,
        left: u64,
    },
    // The user said yes: fill at the next activation.
    Approved,
    // The user said no.
    Declined,
    // Filled: `filled` articles added, `left` still left out.
    Filled {
        filled: u64,
        left: u64,
    },
    // Looked at and of no use, in these words.
    Unusable(String),
}

impl FillState {
    pub fn of(metadata: &[(String, String)]) -> Self {
        let Some(value) = metadata
            .iter()
            .rev()
            .find(|(key, _)| key == INNER_NZB_FILL_ATTRIBUTE_KEY)
            .map(|(_, value)| value.as_str())
        else {
            return Self::Untried;
        };
        let numbers = |rest: &str| -> Vec<u64> {
            rest.split('/')
                .filter_map(|n| n.trim().parse().ok())
                .collect()
        };
        match value.split_once(':') {
            Some(("offered", rest)) => match numbers(rest)[..] {
                [filled, skipped, left] => Self::Offered {
                    filled,
                    skipped,
                    left,
                },
                _ => Self::Untried,
            },
            Some(("filled", rest)) => match numbers(rest)[..] {
                [filled, left] => Self::Filled { filled, left },
                _ => Self::Filled { filled: 0, left: 0 },
            },
            Some(("unusable", reason)) => Self::Unusable(reason.to_string()),
            _ => match value {
                "approved" => Self::Approved,
                "declined" => Self::Declined,
                _ => Self::Untried,
            },
        }
    }

    pub fn value(&self) -> Option<String> {
        Some(match self {
            Self::Untried => return None,
            Self::Offered {
                filled,
                skipped,
                left,
            } => format!("offered:{filled}/{skipped}/{left}"),
            Self::Approved => "approved".to_string(),
            Self::Declined => "declined".to_string(),
            Self::Filled { filled, left } => format!("filled:{filled}/{left}"),
            Self::Unusable(reason) => format!("unusable:{reason}"),
        })
    }

    // What a job given up for what its NZB leaves out adds to its error.
    pub fn error_note(&self) -> Option<String> {
        match self {
            Self::Untried | Self::Approved => None,
            // Resumed without an answer: taken as a no.
            Self::Offered { .. } => {
                Some("resumed without filling from the post's own NZB".to_string())
            }
            Self::Declined => Some("filling from the post's own NZB was declined".to_string()),
            Self::Filled { filled, left } => Some(format!(
                "the post's own NZB filled {filled} articles, {left} still left out"
            )),
            Self::Unusable(reason) => Some(reason.clone()),
        }
    }
}

// What the activation's look at the post's own NZB came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillOutcome {
    // The stored NZB was rewritten and the job's lists are the filled ones.
    Filled {
        filled: u64,
        left: u64,
        total_bytes: u64,
    },
    // Asked first: nothing written, the job is to be paused and offered.
    Offered {
        filled: u64,
        skipped: u64,
        left: u64,
    },
    // Looked at and of no use, in these words: the job is given up.
    Unusable(String),
}

impl FillOutcome {
    pub fn state(&self) -> FillState {
        match self {
            Self::Filled { filled, left, .. } => FillState::Filled {
                filled: *filled,
                left: *left,
            },
            Self::Offered {
                filled,
                skipped,
                left,
            } => FillState::Offered {
                filled: *filled,
                skipped: *skipped,
                left: *left,
            },
            Self::Unusable(reason) => FillState::Unusable(reason.clone()),
        }
    }
}

// Everything the activation needs to look, taken on the pipeline thread.
pub(crate) struct FillAsk {
    pub(crate) state: FillState,
    // The job's files, their lists not read: names, roles, groups.
    pub(crate) spec: JobSpec,
    pub(crate) nntp: Arc<NntpClient>,
}

impl Pipeline {
    // What a job's activation is to know to look at its post's own NZB, or
    // nothing when it is not to look: the job started before, or it was
    // looked at or answered already. An approved job looks again, to fill.
    pub(crate) fn inner_nzb_fill_ask(&self, job_id: JobId, fresh: bool) -> Option<FillAsk> {
        let state = self.jobs.get(&job_id)?;
        let fill = FillState::of(&state.spec.metadata);
        let looks = match fill {
            FillState::Untried => fresh,
            FillState::Approved => true,
            _ => false,
        };
        if !looks {
            return None;
        }
        let mut spec = state.spec.clone();
        for file in &mut spec.files {
            file.segments = Vec::new();
        }
        Some(FillAsk {
            state: fill,
            spec,
            nntp: Arc::clone(&self.nntp),
        })
    }

    // Keep what the look came to in the job's metadata, and on its stored
    // row. A filled job counts its new size. True when the job was offered:
    // it is then held, paused, its lists let go, for the user's answer.
    pub(crate) fn record_inner_nzb_fill(&mut self, job_id: JobId, outcome: &FillOutcome) -> bool {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        let update = crate::jobs::JobUpdate {
            metadata_edits: vec![(
                INNER_NZB_FILL_ATTRIBUTE_KEY.to_string(),
                outcome.state().value(),
            )],
            ..Default::default()
        };
        update.apply_to_spec(&mut state.spec);
        if let FillOutcome::Filled { total_bytes, .. } = outcome {
            state.spec.total_bytes = *total_bytes;
        }
        let offered = matches!(outcome, FillOutcome::Offered { .. });
        if offered {
            // Nothing was built: the job goes back to waiting, holding the
            // summaries it was activated with.
            let summaries = match &mut state.articles {
                crate::jobs::model::JobArticles::Activating(summaries, _) => {
                    std::mem::take(summaries)
                }
                _ => crate::jobs::model::FileSummaries::default(),
            };
            state.articles = crate::jobs::model::JobArticles::Queued(summaries);
        }
        if let Err(error) = self
            .db
            .try_queue_job_write(job_id, "inner_nzb_fill", move |db| {
                db.update_active_job(job_id, &update)
            })
        {
            tracing::warn!(job_id = job_id.0, %error, "could not record the post's own NZB fill");
        }
        match outcome {
            FillOutcome::Filled { filled, left, .. } => tracing::info!(
                job_id = job_id.0,
                filled,
                left,
                "filled the articles its NZB leaves out from the post's own NZB"
            ),
            FillOutcome::Offered {
                filled, skipped, ..
            } => tracing::info!(
                job_id = job_id.0,
                filled,
                skipped,
                "the post's own NZB can fill what its NZB leaves out; waiting for an answer"
            ),
            FillOutcome::Unusable(reason) => {
                tracing::info!(job_id = job_id.0, reason = %reason, "the post's own NZB is of no use")
            }
        }
        if offered {
            if let Err(error) = self.pause_job_runtime(job_id) {
                tracing::warn!(job_id = job_id.0, %error, "could not pause the job offered a fill");
            }
            let _ = self.shared_state.queue.set_job_paused(job_id, true);
            self.report_job(job_id);
        }
        offered
    }
}

// The bytes the job's lists leave out, its health then, and its critical
// line, as the pipeline's weighing counts them for a job that has lost
// nothing yet.
fn weigh(spec: &JobSpec) -> (crate::pipeline::post_health::Unlisted, u32, u32) {
    let found = crate::pipeline::post_health::unlisted(spec);
    let total = spec.total_bytes.saturating_add(found.bytes);
    let health = super::health_milli(total, found.bytes);
    let critical = Pipeline::critical_health_milli(
        total,
        crate::pipeline::post_health::declared_recovery_bytes(spec),
    );
    (found, health, critical)
}

// The file the post carries its own NZB in: one named `*.nzb` whose own list
// skips nothing, the largest when there are several.
fn own_nzb_file(spec: &JobSpec) -> Option<usize> {
    spec.files
        .iter()
        .enumerate()
        .filter(|(_, file)| file.filename.to_ascii_lowercase().ends_with(".nzb"))
        .filter(|(_, file)| {
            let last = file
                .segments
                .iter()
                .map(|segment| segment.article_number)
                .max()
                .unwrap_or(0);
            !file.segments.is_empty() && last as usize == file.segments.len()
        })
        .max_by_key(|(_, file)| file.segments.len())
        .map(|(index, _)| index)
}

// Look at the post's own NZB for a job whose lists `lists` were just read,
// in the job's activation, off the pipeline thread. `None` leaves the job as
// it was: it is not short past its recovery, or it carries no NZB of its
// own. Filling, the stored NZB and outline are rewritten and `lists` are the
// filled lists.
pub(crate) fn look(
    db: &crate::Database,
    job_id: JobId,
    ask: FillAsk,
    lists: &mut Vec<Vec<SegmentSpec>>,
    runtime: &tokio::runtime::Handle,
) -> Option<FillOutcome> {
    let FillAsk {
        state,
        mut spec,
        nntp,
    } = ask;
    let mode = stored_mode(db);
    if mode == InnerNzbFillMode::Off && state != FillState::Approved {
        return None;
    }
    for (file, list) in spec.files.iter_mut().zip(lists.iter()) {
        file.segments = list.clone();
    }
    let (_, health, critical) = weigh(&spec);
    if health >= critical {
        return None;
    }
    let own = own_nzb_file(&spec)?;
    let unusable = |words: String| Some(FillOutcome::Unusable(words));

    let file = &spec.files[own];
    let bytes = match fetch_file(&nntp, file, runtime) {
        Ok(bytes) => bytes,
        Err(error) => {
            return unusable(format!("the post's own NZB could not be fetched: {error}"));
        }
    };
    let inner = match weaver_nzb::parse_nzb(&bytes) {
        Ok(inner) => inner,
        Err(error) => return unusable(format!("the post's own NZB could not be read: {error}")),
    };
    let stored = match read_stored_nzb(db, job_id) {
        Ok(stored) => stored,
        Err(error) => return unusable(format!("the job's NZB could not be read: {error}")),
    };
    let fill = weaver_nzb::fill_from(&stored, &inner);
    if fill.filled == 0 {
        return unusable(format!(
            "the post's own NZB has none of the {} articles its NZB leaves out",
            fill.skipped_before
        ));
    }

    // Weigh the job as it would be filled.
    let filled_spec = crate::ingest::nzb_to_spec(
        &fill.nzb,
        std::path::Path::new(""),
        spec.category.clone(),
        spec.metadata.clone(),
    );
    let (_, after, critical_after) = weigh(&filled_spec);
    if after < critical_after {
        return unusable(format!(
            "the post's own NZB fills {} of the {} articles its NZB leaves out; {} still left out is more than its recovery covers",
            fill.filled, fill.skipped_before, fill.skipped_after
        ));
    }
    if mode == InnerNzbFillMode::Ask && state != FillState::Approved {
        return Some(FillOutcome::Offered {
            filled: fill.filled,
            skipped: fill.skipped_before,
            left: fill.skipped_after,
        });
    }

    if let Err(error) = replace_stored_nzb(db, job_id, &fill.nzb) {
        return unusable(format!("the filled NZB could not be stored: {error}"));
    }
    match crate::pipeline::activation::read_stored_spec(
        db,
        job_id,
        spec.category.clone(),
        spec.metadata.clone(),
    ) {
        Ok(read) if read.files.len() == lists.len() => {
            let total_bytes = read.total_bytes;
            *lists = read.files.into_iter().map(|file| file.segments).collect();
            Some(FillOutcome::Filled {
                filled: fill.filled,
                left: fill.skipped_after,
                total_bytes,
            })
        }
        Ok(_) => unusable("the filled NZB lists other files than the job's".to_string()),
        Err(error) => unusable(format!("the filled NZB could not be read back: {error}")),
    }
}

// The job's stored NZB, parsed.
fn read_stored_nzb(db: &crate::Database, job_id: JobId) -> Result<Nzb, String> {
    let (_, nzb_zstd) = db
        .load_active_job_persisted_nzb(job_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "the job has no stored row".to_string())?;
    let nzb_zstd = nzb_zstd.ok_or_else(|| "the job has no stored NZB".to_string())?;
    crate::ingest::parse_persisted_nzb_bytes(&nzb_zstd).map_err(|error| error.to_string())
}

// Store `nzb` as the job's NZB, and its outline with it, in one write.
fn replace_stored_nzb(db: &crate::Database, job_id: JobId, nzb: &Nzb) -> Result<(), String> {
    let xml = weaver_nzb::write_nzb(nzb);
    let nzb_zstd =
        crate::ingest::compress_nzb_bytes(xml.as_bytes()).map_err(|error| error.to_string())?;
    let outline = crate::ingest::outline::NzbOutline::of(nzb);
    db.replace_job_nzb(job_id, nzb_zstd, &outline)
        .map_err(|error| error.to_string())
}

// The decoded bytes of one small file of the job, its articles fetched one
// after the other in number order. An article a server does not have is
// asked of the next one.
fn fetch_file(
    nntp: &Arc<NntpClient>,
    file: &crate::jobs::model::FileSpec,
    runtime: &tokio::runtime::Handle,
) -> Result<Vec<u8>, String> {
    const DIAL_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
    const MAX_BYTES: u64 = 64 * 1024 * 1024;
    let mut segments: Vec<&SegmentSpec> = file.segments.iter().collect();
    segments.sort_by_key(|segment| segment.article_number);
    if segments
        .iter()
        .map(|segment| u64::from(segment.bytes))
        .sum::<u64>()
        > MAX_BYTES
    {
        return Err("it is too large to be an NZB".to_string());
    }
    let dial = |exclude: ServerSet| {
        let give_up = std::time::Instant::now() + DIAL_WAIT;
        loop {
            match super::download::owned_lane::acquire_owned_lane_through_contention(
                || true,
                || nntp.try_acquire_blocking_body_lane(&file.groups, exclude),
            ) {
                Err(error)
                    if error.should_requeue_owned_work() && std::time::Instant::now() < give_up =>
                {
                    runtime.block_on(tokio::time::sleep(std::time::Duration::from_millis(250)));
                }
                outcome => return outcome.map_err(|error| format!("{error:?}")),
            }
        }
    };
    let mut out = Vec::new();
    let mut lane = None;
    for segment in segments {
        let mut exclude = ServerSet::EMPTY;
        let mut last_error = String::new();
        let body = loop {
            let open = match lane.take() {
                Some(open) => open,
                None => match dial(exclude) {
                    Ok(open) => open,
                    Err(error) => {
                        return Err(if last_error.is_empty() {
                            error
                        } else {
                            last_error
                        });
                    }
                },
            };
            let mut open = open;
            if open.adopt_groups(&file.groups).is_err() {
                // A server that will not take the file's groups is passed
                // over for this article.
                if !exclude.insert(open.server_id().0) {
                    return Err("no server would select the file's groups".to_string());
                }
                continue;
            }
            let trace = open.fetch_decoded_sequential(segment.message_ids.in_use());
            nntp.record_blocking_attempts(&trace.attempts);
            let server = open.server_id().0;
            if open.is_healthy() {
                lane = Some(open);
            }
            match trace.result {
                Ok(body) => break body,
                Err(error) => {
                    last_error = format!("article {}: {error:?}", segment.article_number);
                    // Asked of the next server; a server asked once is not
                    // asked again for this article.
                    if !exclude.insert(server) {
                        return Err(last_error);
                    }
                    lane = None;
                }
            }
        };
        for part in body.decoded {
            out.extend_from_slice(&part);
        }
    }
    Ok(out)
}
