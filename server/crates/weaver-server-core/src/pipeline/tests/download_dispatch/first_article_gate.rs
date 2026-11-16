// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A job of `files` files, each of `segments` articles.
fn multi_file_job_spec(name: &str, files: usize, segments: u32) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (files as u64) * (segments as u64) * 512,
        category: None,
        metadata: vec![],
        files: (0..files)
            .map(|file_index| FileSpec {
                filename: format!("copper-meridian.part{file_index:02}.bin"),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..segments)
                    .map(|ordinal| {
                        segment_spec! {
                            number: ordinal,
                            bytes: 512u32,
                            message_id: format!("copper-{file_index}-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn job_failed(pipeline: &Pipeline, job_id: JobId) -> Option<String> {
    match job_status_for_assert(pipeline, job_id) {
        Some(JobStatus::Failed { error, .. }) => Some(error),
        _ => None,
    }
}

// A handful of files is not a sample. A small post that loses its first file
// is an ordinary damaged post, and the health arithmetic rules on it.
#[tokio::test]
async fn a_small_post_is_not_judged_on_its_first_articles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41702);
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Too Few To Judge", 6, 4),
    )
    .await;

    // Only the first articles are answered for; the rest of the queue is left
    // alone, so nothing but the gate could have ruled here.
    let first_articles: Vec<SegmentId> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .first_articles()
        .collect();
    assert_eq!(first_articles.len(), 6);
    for segment_id in first_articles {
        pipeline.book_terminal_segment(segment_id, SegmentTerminalState::Missing);
    }

    assert!(
        job_failed(&pipeline, job_id).is_none(),
        "a six-file post is not a post the sample can speak for"
    );
}

// `files` files whose NZB entries list only the article numbers in
// `numbers`; ordinals stay dense, as ingest gives them.
fn short_listed_job_spec(name: &str, files: usize, numbers: &[u32]) -> JobSpec {
    let mut spec = multi_file_job_spec(name, files, numbers.len() as u32);
    for file in &mut spec.files {
        for (segment, &number) in file.segments.iter_mut().zip(numbers) {
            segment.article_number = number;
        }
    }
    spec
}

// An NZB that lists six parts in ten of every file, with no recovery named,
// is past saving before a byte is fetched: it is given up as its articles
// are built, every short file abandoned, instead of downloading at full health
// and failing its repair.
#[tokio::test]
async fn a_post_its_nzb_leaves_past_recovery_is_given_up_as_its_articles_are_built() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41703);
    insert_active_job(
        &mut pipeline,
        job_id,
        short_listed_job_spec("Listed Short", 12, &[1, 2, 7, 8, 9, 10]),
    )
    .await;

    pipeline.weigh_unlisted_articles(job_id);

    let reason = pipeline.post_probes.dead.get(&job_id).cloned();
    assert!(
        reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("NZB leaves out 48 articles")),
        "{reason:?}"
    );
    assert_eq!(
        pipeline
            .post_probes
            .verdicts
            .get(&job_id)
            .map(|verdict| verdict.abandoned_files.len()),
        Some(12)
    );
    // Every file abandoned, the download is over: nothing more is weighed.
    assert_eq!(pipeline.unlisted_bytes(job_id), 0);
}

// One part left out of a hundred and twenty is a hole, not a dead post: the
// job keeps downloading, its health counting the part as lost.
#[tokio::test]
async fn a_post_its_nzb_leaves_a_little_short_keeps_downloading() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41704);
    let mut spec = multi_file_job_spec("Listed Nearly Whole", 12, 10);
    // The first file's NZB entry skips part 5: it lists 1-4 and 6-11.
    for (segment, number) in spec.files[0].segments.iter_mut().zip((1..=4).chain(6..=11)) {
        segment.article_number = number;
    }
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.weigh_unlisted_articles(job_id);

    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
    assert!(!pipeline.post_probes.verdicts.contains_key(&job_id));
    assert_eq!(pipeline.unlisted_bytes(job_id), 512);
}

// Added as any NZB is, through the queue: its article lists are let go once
// stored and read back only when it is first handed work. The weighing is
// done then, with the lists in hand, not when the job takes its slot and
// holds only summaries.
#[tokio::test]
async fn a_queued_post_its_nzb_leaves_past_recovery_is_given_up_when_first_handed_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41705);
    let spec = short_listed_job_spec("Queued Short", 12, &[1, 2, 7, 8, 9, 10]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            job_id,
            spec,
            std::path::PathBuf::from("queued-short.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    assert!(matches!(
        pipeline.jobs[&job_id].articles,
        crate::jobs::model::JobArticles::Queued(_)
    ));
    // Waiting, it holds summaries: nothing is counted, nothing given up.
    pipeline.weigh_unlisted_articles(job_id);
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));

    activate_job(&mut pipeline, job_id).await;

    let reason = pipeline.post_probes.dead.get(&job_id).cloned();
    assert!(
        reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("NZB leaves out 48 articles")),
        "{reason:?}"
    );
    assert_eq!(
        pipeline
            .post_probes
            .verdicts
            .get(&job_id)
            .map(|verdict| verdict.abandoned_files.len()),
        Some(12)
    );
}

// A job a little short keeps what was counted while it is parked: its lists
// let go on a pause, the bytes still weigh in its health, and are counted
// again as its articles are built on resume.
#[tokio::test]
async fn a_post_a_little_short_keeps_its_count_across_a_park() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41706);
    let mut spec = multi_file_job_spec("Queued Nearly Whole", 12, 10);
    for (segment, number) in spec.files[0].segments.iter_mut().zip((1..=4).chain(6..=11)) {
        segment.article_number = number;
    }
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            job_id,
            spec,
            std::path::PathBuf::from("queued-nearly-whole.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    activate_job(&mut pipeline, job_id).await;
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
    assert_eq!(pipeline.unlisted_bytes(job_id), 512);

    pipeline.release_job_articles(job_id);
    pipeline.weigh_unlisted_articles(job_id);
    assert_eq!(
        pipeline.unlisted_bytes(job_id),
        512,
        "a parked job keeps its count"
    );

    activate_job(&mut pipeline, job_id).await;
    assert_eq!(pipeline.unlisted_bytes(job_id), 512);
    assert!(!pipeline.post_probes.dead.contains_key(&job_id));
}
