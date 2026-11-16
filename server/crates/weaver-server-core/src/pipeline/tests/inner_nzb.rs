// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A post whose NZB leaves out past its recovery, carrying a complete NZB of
// its own: the own NZB is fetched from a loopback server as the job is first
// handed work, and the job is filled, offered, or given up as the setting and
// the user's answer say.

use super::*;
use crate::jobs::model::{JobArticles, MessageIds, RunState, SegmentSpec};
use crate::pipeline::inner_nzb::{FillState, INNER_NZB_FILL_ATTRIBUTE_KEY, INNER_NZB_FILL_SETTING};
use crate::speed_test::fixture::{FixtureArticle, NntpFixture};

const FILES: u32 = 12;
const PARTS: u32 = 10;
// The article numbers the indexer's NZB lists of each data file.
const LISTED: [u32; 6] = [1, 2, 7, 8, 9, 10];
const OWN_PARTS: u32 = 2;

fn segment(ordinal: u32, number: u32, id: String) -> SegmentSpec {
    SegmentSpec {
        ordinal,
        article_number: number,
        bytes: 512,
        message_ids: MessageIds::new(vec![id]),
    }
}

// The job as the indexer's NZB has it: twelve volumes listing six parts of
// ten, and the post's own NZB, listed whole.
fn short_spec() -> JobSpec {
    let mut files: Vec<FileSpec> = (0..FILES)
        .map(|file| FileSpec {
            filename: format!("copper-meridian.part{:02}.rar", file + 1),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: LISTED
                .iter()
                .zip(0u32..)
                .map(|(&number, ordinal)| {
                    segment(ordinal, number, format!("short-{file}-{number}@x"))
                })
                .collect(),
        })
        .collect();
    files.push(FileSpec {
        filename: "copper-meridian.nzb".to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: (1..=OWN_PARTS)
            .map(|number| segment(number - 1, number, format!("own-{number}@x")))
            .collect(),
    });
    let total_bytes = files
        .iter()
        .flat_map(|file| &file.segments)
        .map(|segment| u64::from(segment.bytes))
        .sum();
    JobSpec {
        name: "Copper Meridian".to_string(),
        password: None,
        files,
        total_bytes,
        category: None,
        metadata: vec![],
    }
}

// The post's own NZB: the same volumes, named with three-digit parts, every
// part listed, the numbers in `lacking` left out.
fn own_nzb(lacking: &[u32]) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"#,
    );
    for file in 0..FILES {
        xml.push_str(&format!(
            r#"<file poster="p" date="1700000000" subject="&quot;copper-meridian.part{:03}.rar&quot; yEnc (1/{PARTS})"><groups><group>alt.binaries.test</group></groups><segments>"#,
            file + 1
        ));
        for number in (1..=PARTS).filter(|number| !lacking.contains(number)) {
            xml.push_str(&format!(
                r#"<segment bytes="512" number="{number}">repost-{file}-{number}@x</segment>"#
            ));
        }
        xml.push_str("</segments></file>");
    }
    xml.push_str("</nzb>");
    xml
}

// A loopback server holding the own NZB in its articles, and the pipeline's
// client pointed at it.
async fn serve_own_nzb(pipeline: &mut Pipeline, xml: &str) -> NntpFixture {
    let bytes = xml.as_bytes();
    let half = bytes.len().div_ceil(OWN_PARTS as usize);
    let articles = bytes
        .chunks(half)
        .zip(1..)
        .map(|(chunk, number)| FixtureArticle {
            message_id: format!("own-{number}@x"),
            payload: chunk.to_vec(),
        })
        .collect();
    let fixture = NntpFixture::start(articles, None, None).await;
    pipeline.nntp = Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: fixture.addr.port(),
                tls: false,
                ..Default::default()
            },
            max_connections: 2,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        }],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    }));
    fixture
}

async fn add_short_job(pipeline: &mut Pipeline, job_id: JobId) {
    let spec = short_spec();
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            job_id,
            spec,
            PathBuf::from("copper-meridian.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    settle(pipeline).await;
}

async fn settle(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
}

fn fill_state(pipeline: &Pipeline, job_id: JobId) -> FillState {
    FillState::of(&pipeline.jobs[&job_id].spec.metadata)
}

fn listed_numbers(pipeline: &Pipeline, job_id: JobId, file: usize) -> Vec<u32> {
    pipeline.jobs[&job_id].spec.files[file]
        .segments
        .iter()
        .map(|segment| segment.article_number)
        .collect()
}

// Set to fill without asking, the job is filled from its own NZB as it is
// first handed work: each volume gets the four parts it lacked, from the
// repost, and keeps the six it listed. It is not given up, and the stored NZB
// is the filled one, so the job reads the same lists back after a park.
#[tokio::test]
async fn set_to_fill_a_short_post_is_filled_from_its_own_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let _fixture = serve_own_nzb(&mut pipeline, &own_nzb(&[])).await;
    pipeline
        .db
        .set_setting(INNER_NZB_FILL_SETTING, "auto")
        .unwrap();
    let job_id = JobId(42001);
    add_short_job(&mut pipeline, job_id).await;

    activate_job(&mut pipeline, job_id).await;

    assert_eq!(
        fill_state(&pipeline, job_id),
        FillState::Filled {
            filled: 48,
            left: 0
        }
    );
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
    assert_eq!(pipeline.unlisted_bytes(job_id), 0);
    assert_eq!(
        listed_numbers(&pipeline, job_id, 0),
        (1..=PARTS).collect::<Vec<_>>()
    );
    let ids: Vec<String> = pipeline.jobs[&job_id].spec.files[0]
        .segments
        .iter()
        .map(|segment| segment.message_ids.in_use().to_string())
        .collect();
    assert_eq!(ids[0], "short-0-1@x", "what the NZB listed is kept");
    assert_eq!(
        ids[2], "repost-0-3@x",
        "a skipped part comes from the repost"
    );
    assert!(matches!(
        pipeline.jobs[&job_id].articles,
        JobArticles::Active(_)
    ));

    // Parked and handed work again, the job reads the filled NZB back.
    pipeline.release_job_articles(job_id);
    activate_job(&mut pipeline, job_id).await;
    assert!(matches!(
        pipeline.jobs[&job_id].articles,
        JobArticles::Active(_)
    ));
    assert_eq!(
        listed_numbers(&pipeline, job_id, 5),
        (1..=PARTS).collect::<Vec<_>>()
    );
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
}

// Set to ask, which is the default, the job is offered the fill and paused,
// nothing written. Approved and resumed, it is filled as it is handed work.
#[tokio::test]
async fn asked_first_a_short_post_waits_paused_and_is_filled_once_approved() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let _fixture = serve_own_nzb(&mut pipeline, &own_nzb(&[])).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let job_id = JobId(42002);
    add_short_job(&mut pipeline, job_id).await;

    activate_job(&mut pipeline, job_id).await;

    assert_eq!(
        fill_state(&pipeline, job_id),
        FillState::Offered {
            filled: 48,
            skipped: 48,
            left: 0
        }
    );
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
    assert_eq!(pipeline.jobs[&job_id].run_state, RunState::Paused);
    assert!(matches!(
        pipeline.jobs[&job_id].articles,
        JobArticles::Queued(_)
    ));

    handle
        .update_job(
            job_id,
            crate::jobs::JobUpdate {
                metadata_edits: vec![(
                    INNER_NZB_FILL_ATTRIBUTE_KEY.to_string(),
                    Some("approved".to_string()),
                )],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle.resume_job(job_id).await.unwrap();
    settle(&mut pipeline).await;
    activate_job(&mut pipeline, job_id).await;

    assert_eq!(
        fill_state(&pipeline, job_id),
        FillState::Filled {
            filled: 48,
            left: 0
        }
    );
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
    assert_eq!(
        listed_numbers(&pipeline, job_id, 11),
        (1..=PARTS).collect::<Vec<_>>()
    );
}

// Declined and resumed, the job is given up as it would have been, the
// answer in its error.
#[tokio::test]
async fn declined_a_short_post_is_given_up_saying_so() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let _fixture = serve_own_nzb(&mut pipeline, &own_nzb(&[])).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let job_id = JobId(42003);
    add_short_job(&mut pipeline, job_id).await;
    activate_job(&mut pipeline, job_id).await;
    assert!(matches!(
        fill_state(&pipeline, job_id),
        FillState::Offered { .. }
    ));

    handle
        .update_job(
            job_id,
            crate::jobs::JobUpdate {
                metadata_edits: vec![(
                    INNER_NZB_FILL_ATTRIBUTE_KEY.to_string(),
                    Some("declined".to_string()),
                )],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle.resume_job(job_id).await.unwrap();
    settle(&mut pipeline).await;
    activate_job(&mut pipeline, job_id).await;

    let reason = pipeline
        .post_probes
        .dead
        .get(&job_id)
        .cloned()
        .unwrap_or_default();
    assert!(reason.starts_with("NZB leaves out 48 articles"), "{reason}");
    assert!(
        reason.ends_with("filling from the post's own NZB was declined"),
        "{reason}"
    );
}

// An own NZB that lacks the same parts is no use: the job is given up at
// once, never offered, the reason in its error.
#[tokio::test]
async fn an_own_nzb_that_lacks_the_same_parts_is_no_use_and_never_offered() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let _fixture = serve_own_nzb(&mut pipeline, &own_nzb(&[3, 4, 5, 6])).await;
    let job_id = JobId(42004);
    add_short_job(&mut pipeline, job_id).await;

    activate_job(&mut pipeline, job_id).await;

    assert!(matches!(
        fill_state(&pipeline, job_id),
        FillState::Unusable(_)
    ));
    assert_eq!(pipeline.jobs[&job_id].run_state, RunState::Active);
    let reason = pipeline
        .post_probes
        .dead
        .get(&job_id)
        .cloned()
        .unwrap_or_default();
    assert!(
        reason.ends_with("the post's own NZB has none of the 48 articles its NZB leaves out"),
        "{reason}"
    );
}

// Set off, the own NZB is never fetched and the job is given up as before.
#[tokio::test]
async fn set_off_the_own_nzb_is_never_fetched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let fixture = serve_own_nzb(&mut pipeline, &own_nzb(&[])).await;
    pipeline
        .db
        .set_setting(INNER_NZB_FILL_SETTING, "off")
        .unwrap();
    let job_id = JobId(42005);
    add_short_job(&mut pipeline, job_id).await;

    activate_job(&mut pipeline, job_id).await;

    assert_eq!(fill_state(&pipeline, job_id), FillState::Untried);
    assert_eq!(*fixture.served().borrow(), 0);
    let reason = pipeline
        .post_probes
        .dead
        .get(&job_id)
        .cloned()
        .unwrap_or_default();
    assert!(reason.starts_with("NZB leaves out 48 articles"), "{reason}");
}

// Resumed without an answer, the offer is taken as a no: the job is given up,
// its error saying so.
#[tokio::test]
async fn resumed_without_an_answer_a_short_post_is_given_up_saying_so() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let _fixture = serve_own_nzb(&mut pipeline, &own_nzb(&[])).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let job_id = JobId(42006);
    add_short_job(&mut pipeline, job_id).await;
    activate_job(&mut pipeline, job_id).await;
    assert!(matches!(
        fill_state(&pipeline, job_id),
        FillState::Offered { .. }
    ));

    handle.resume_job(job_id).await.unwrap();
    settle(&mut pipeline).await;
    activate_job(&mut pipeline, job_id).await;

    let reason = pipeline
        .post_probes
        .dead
        .get(&job_id)
        .cloned()
        .unwrap_or_default();
    assert!(
        reason.ends_with("resumed without filling from the post's own NZB"),
        "{reason}"
    );
}
