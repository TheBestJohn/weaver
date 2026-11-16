// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Filling the articles an NZB leaves out from the NZB its own post carries.
//
// An indexer that missed part of a posting writes an NZB whose files skip
// article numbers. Some posters put a complete NZB in the post itself, often
// pointing at a repost of the same files. Where the repost names the same
// file, split into the same articles, its article with a skipped number holds
// the bytes the short NZB is missing, and that article alone is added to the
// file. Nothing the short NZB lists is replaced.
//
// Files are matched by the name their subject gives, compared without case,
// punctuation, or zero-padding (`part01` and `part001` are one name): an
// indexer rewrites subjects, and a repost numbers its volumes its own way. A
// name two of the inner NZB's files share is matched to neither. A file is
// filled only where the inner one is at least as long, so a file cut into
// different articles is left as it is.

use std::collections::{BTreeSet, HashMap};

use crate::types::{Nzb, NzbFile, NzbSegment};

// What filling one NZB from another came to.
#[derive(Debug, Clone)]
pub struct Fill {
    // The short NZB with every article it could be given added, its files in
    // their order and each file's articles sorted by number.
    pub nzb: Nzb,
    // Articles the short NZB skipped before, and still skips.
    pub skipped_before: u64,
    pub skipped_after: u64,
    // Articles added, and the files they went to, by index.
    pub filled: u64,
    pub filled_files: BTreeSet<u32>,
}

// The article numbers a file's NZB entry skips: from 1 to its last listed
// number, every number it does not list. A file numbered past four times the
// parts it lists is not counted, as the pipeline's weighing does not count
// it: that is numbering the NZB did not write as a count.
pub fn skipped_numbers(file: &NzbFile) -> Vec<u32> {
    let Some(last) = file.segments.iter().map(|segment| segment.number).max() else {
        return Vec::new();
    };
    let listed = file.segments.len() as u64;
    if u64::from(last) > listed.saturating_mul(4) {
        return Vec::new();
    }
    let present: BTreeSet<u32> = file.segments.iter().map(|segment| segment.number).collect();
    (1..=last)
        .filter(|number| !present.contains(number))
        .collect()
}

// The name a file is matched by: its subject's file name, lower-cased, with
// only letters and digits kept and each run of digits without leading zeros.
pub fn match_name(file: &NzbFile) -> Option<String> {
    let name = file
        .extract_filename()
        .map(|(name, _)| name)
        .or_else(|| file.filename().map(str::to_string))?;
    let mut key = String::with_capacity(name.len());
    let mut digits = String::new();
    let flush = |digits: &mut String, key: &mut String| {
        if !digits.is_empty() {
            let trimmed = digits.trim_start_matches('0');
            key.push_str(if trimmed.is_empty() { "0" } else { trimmed });
            digits.clear();
        }
    };
    for ch in name.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            flush(&mut digits, &mut key);
            if ch.is_alphanumeric() {
                key.push(ch);
            }
        }
    }
    flush(&mut digits, &mut key);
    (!key.is_empty()).then_some(key)
}

// Fill `short`'s skipped articles from `inner`.
pub fn fill_from(short: &Nzb, inner: &Nzb) -> Fill {
    let mut by_name: HashMap<String, Option<&NzbFile>> = HashMap::new();
    for file in &inner.files {
        if let Some(key) = match_name(file) {
            by_name
                .entry(key)
                .and_modify(|seen| *seen = None)
                .or_insert(Some(file));
        }
    }

    let mut nzb = short.clone();
    let mut fill = Fill {
        nzb: short.clone(),
        skipped_before: 0,
        skipped_after: 0,
        filled: 0,
        filled_files: BTreeSet::new(),
    };
    for (file, index) in nzb.files.iter_mut().zip(0u32..) {
        let skipped = skipped_numbers(file);
        fill.skipped_before += skipped.len() as u64;
        if skipped.is_empty() {
            continue;
        }
        let source = match_name(file)
            .and_then(|key| by_name.get(&key).copied().flatten())
            .filter(|other| {
                let last = |f: &NzbFile| f.segments.iter().map(|s| s.number).max().unwrap_or(0);
                last(other) >= last(file)
            });
        let Some(source) = source else {
            fill.skipped_after += skipped.len() as u64;
            continue;
        };
        let theirs: HashMap<u32, &NzbSegment> = source
            .segments
            .iter()
            .map(|segment| (segment.number, segment))
            .collect();
        let mut added = 0u64;
        for number in &skipped {
            match theirs.get(number) {
                Some(segment) => {
                    file.segments.push((*segment).clone());
                    added += 1;
                }
                None => fill.skipped_after += 1,
            }
        }
        if added > 0 {
            file.segments.sort_by_key(|segment| segment.number);
            fill.filled += added;
            fill.filled_files.insert(index);
        }
    }
    fill.nzb = nzb;
    fill
}

// An NZB document for `nzb`, which the parser reads back as the same files,
// articles, groups, and head.
pub fn write_nzb(nzb: &Nzb) -> String {
    fn escape(text: &str, out: &mut String) {
        for ch in text.chars() {
            match ch {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                '"' => out.push_str("&quot;"),
                '\'' => out.push_str("&apos;"),
                _ => out.push(ch),
            }
        }
    }
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE nzb PUBLIC \"-//newzBin//DTD NZB 1.1//EN\" \"http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd\">\n\
         <nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n",
    );
    let meta = &nzb.meta;
    let passwords: Vec<&String> = if meta.passwords.is_empty() {
        meta.password.iter().collect()
    } else {
        meta.passwords.iter().collect()
    };
    if meta.title.is_some() || !passwords.is_empty() || !meta.tags.is_empty() {
        out.push_str("<head>\n");
        let tag = |key: &str, value: &str, out: &mut String| {
            out.push_str("<meta type=\"");
            escape(key, out);
            out.push_str("\">");
            escape(value, out);
            out.push_str("</meta>\n");
        };
        if let Some(title) = &meta.title {
            tag("title", title, &mut out);
        }
        for password in passwords {
            tag("password", password, &mut out);
        }
        for (key, value) in &meta.tags {
            tag(key, value, &mut out);
        }
        out.push_str("</head>\n");
    }
    for file in &nzb.files {
        out.push_str("<file poster=\"");
        escape(&file.poster, &mut out);
        out.push_str(&format!("\" date=\"{}\" subject=\"", file.date));
        escape(&file.subject, &mut out);
        out.push_str("\">\n<groups>");
        for group in &file.groups {
            out.push_str("<group>");
            escape(group, &mut out);
            out.push_str("</group>");
        }
        out.push_str("</groups>\n<segments>\n");
        for segment in &file.segments {
            for id in &segment.message_ids {
                out.push_str(&format!(
                    "<segment bytes=\"{}\" number=\"{}\">",
                    segment.bytes, segment.number
                ));
                escape(id, &mut out);
                out.push_str("</segment>\n");
            }
        }
        out.push_str("</segments>\n</file>\n");
    }
    out.push_str("</nzb>\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_nzb;
    use crate::types::NzbMeta;

    fn file(name: &str, numbers: &[u32], tag: &str) -> NzbFile {
        NzbFile {
            poster: "poster <p@example.invalid>".into(),
            subject: format!("Show [1/9] - \"{name}\" yEnc (1/{})", numbers.len()),
            date: 1_700_000_000,
            groups: vec!["alt.binaries.test".into()],
            segments: numbers
                .iter()
                .map(|&number| NzbSegment {
                    number,
                    bytes: 1000,
                    message_ids: vec![format!("{tag}{number}@{name}")],
                })
                .collect(),
        }
    }

    fn nzb(files: Vec<NzbFile>) -> Nzb {
        Nzb {
            meta: NzbMeta::default(),
            files,
        }
    }

    #[test]
    fn a_skipped_article_comes_from_the_same_file_in_the_inner_nzb() {
        let short = nzb(vec![file("Show.part01.rar", &[1, 2, 4, 5], "s")]);
        let inner = nzb(vec![file("Show.part001.rar", &[1, 2, 3, 4, 5], "i")]);
        let fill = fill_from(&short, &inner);
        assert_eq!(
            (fill.skipped_before, fill.filled, fill.skipped_after),
            (1, 1, 0)
        );
        let ids: Vec<_> = fill.nzb.files[0]
            .segments
            .iter()
            .map(|s| s.message_ids[0].as_str())
            .collect();
        assert_eq!(
            ids,
            [
                "s1@Show.part01.rar",
                "s2@Show.part01.rar",
                "i3@Show.part001.rar",
                "s4@Show.part01.rar",
                "s5@Show.part01.rar"
            ]
        );
        assert_eq!(fill.filled_files, BTreeSet::from([0]));
    }

    #[test]
    fn what_the_inner_nzb_lacks_too_stays_skipped() {
        let short = nzb(vec![file("a.rar", &[1, 4], "s")]);
        let inner = nzb(vec![file("a.rar", &[1, 2, 4], "i")]);
        let fill = fill_from(&short, &inner);
        assert_eq!(
            (fill.skipped_before, fill.filled, fill.skipped_after),
            (2, 1, 1)
        );
    }

    #[test]
    fn an_unmatched_shorter_or_ambiguous_file_is_left_as_it_is() {
        let short = nzb(vec![
            file("a.rar", &[1, 3], "s"),
            file("b.rar", &[1, 3], "s"),
            file("c.rar", &[1, 3], "s"),
        ]);
        let inner = nzb(vec![
            file("other.rar", &[1, 2, 3], "i"),
            file("b.rar", &[1, 2], "i"),
            file("c.rar", &[1, 2, 3], "i"),
            file("C.RAR", &[1, 2, 3], "j"),
        ]);
        let fill = fill_from(&short, &inner);
        assert_eq!(
            (fill.skipped_before, fill.filled, fill.skipped_after),
            (3, 0, 3)
        );
        assert!(fill.filled_files.is_empty());
    }

    #[test]
    fn names_match_without_case_punctuation_or_zero_padding() {
        let a = file("The.Show.Season 4.part010.rar", &[1], "x");
        let b = file("the_show_season_4_part10.RAR", &[1], "x");
        assert_eq!(match_name(&a), match_name(&b));
        assert_ne!(
            match_name(&a),
            match_name(&file("The.Show.Season 4.part011.rar", &[1], "x"))
        );
    }

    #[test]
    fn a_written_nzb_reads_back_the_same() {
        let mut short = nzb(vec![
            file("a & <b>.rar", &[1, 2, 5], "s"),
            file("c.par2", &[1], "s"),
        ]);
        short.files[0].segments[1]
            .message_ids
            .push("again@x".into());
        short.meta.title = Some("Title \"quoted\"".into());
        short.meta.password = Some("pw&1".into());
        short.meta.passwords = vec!["pw&1".into(), "two".into()];
        short.meta.tags = vec![("category".into(), "TV".into())];
        let back = parse_nzb(write_nzb(&short).as_bytes()).unwrap();
        assert_eq!(back.meta.title, short.meta.title);
        assert_eq!(back.meta.password, short.meta.password);
        assert_eq!(back.meta.passwords, short.meta.passwords);
        assert_eq!(back.meta.tags, short.meta.tags);
        assert_eq!(back.files.len(), 2);
        for (a, b) in back.files.iter().zip(&short.files) {
            assert_eq!(
                (&a.poster, &a.subject, a.date, &a.groups),
                (&b.poster, &b.subject, b.date, &b.groups)
            );
            let seg = |f: &NzbFile| {
                f.segments
                    .iter()
                    .map(|s| (s.number, s.bytes, s.message_ids.clone()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(seg(a), seg(b));
        }
    }
}
