use regex::Regex;
use std::borrow::Cow;
use std::path::{Component, Path, PathBuf};

pub fn split_frontmatter(raw: &str) -> (Option<String>, String) {
    let open_len = if raw.starts_with("---\r\n") {
        5
    } else if raw.starts_with("---\n") {
        4
    } else {
        return (None, raw.to_string());
    };

    let mut line_start = open_len;
    while line_start <= raw.len() {
        let line_end = raw[line_start..]
            .find('\n')
            .map(|idx| line_start + idx)
            .unwrap_or(raw.len());
        let mut line = &raw[line_start..line_end];
        if line.ends_with('\r') {
            line = &line[..line.len().saturating_sub(1)];
        }

        if line == "---" {
            let fm = strip_single_trailing_newline(&raw[open_len..line_start])
                .replace("\r\n", "\n")
                .trim_end_matches('\r')
                .to_string();
            let body_start = if line_end < raw.len() {
                line_end + 1
            } else {
                line_end
            };
            return (Some(fm), raw[body_start..].to_string());
        }

        if line_end >= raw.len() {
            break;
        }
        line_start = line_end + 1;
    }

    (None, raw.to_string())
}

fn strip_single_trailing_newline(input: &str) -> &str {
    if let Some(stripped) = input.strip_suffix("\r\n") {
        stripped
    } else if let Some(stripped) = input.strip_suffix('\n') {
        stripped
    } else {
        input
    }
}

pub fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;

    for ch in input.chars() {
        let mapped = match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' => ch,
            '_' | '-' | ' ' | '/' | '\\' | '.' => '-',
            _ if ch.is_alphanumeric() => ch.to_ascii_lowercase(),
            _ => '-',
        };

        if mapped == '-' {
            if !prev_dash {
                out.push(mapped);
                prev_dash = true;
            }
        } else {
            out.push(mapped);
            prev_dash = false;
        }
    }

    out.trim_matches('-').to_string()
}

pub fn url_path_from_relative(relative: &Path) -> String {
    let mut parts: Vec<String> = relative
        .components()
        .filter_map(|c| match c {
            Component::Normal(v) => Some(v.to_string_lossy().to_string()),
            _ => None,
        })
        .collect();

    if let Some(last) = parts.last_mut() {
        if let Some(stem) = Path::new(last).file_stem() {
            *last = stem.to_string_lossy().to_string();
        }
    }

    let slugged: Vec<String> = parts
        .into_iter()
        .map(|p| {
            let s = slugify(&p);
            if s.is_empty() { "note".to_string() } else { s }
        })
        .collect();

    if slugged.is_empty() {
        "/".to_string()
    } else {
        format!("/{}/", slugged.join("/"))
    }
}

pub fn strip_obsidian_comments(input: &str) -> String {
    let re = Regex::new(r"(?s)%%.*?%%").expect("valid regex");
    re.replace_all(input, "").to_string()
}

pub fn convert_highlights(input: &str) -> String {
    let re = Regex::new(r"==([^=\n][^\n]*?)==").expect("valid regex");
    re.replace_all(input, "<mark>$1</mark>").to_string()
}

pub fn strip_dataview_inline_fields(input: &str) -> String {
    let field_start_re = Regex::new(r"[A-Za-z0-9_-]+::").expect("valid regex");
    let trailing_newline = input.ends_with('\n');
    let mut body = Vec::new();

    for line in input.lines() {
        let normalized = line.trim_end_matches('\r');
        let (stripped_line, _fields) =
            extract_dataview_fields_from_line(normalized, &field_start_re);
        body.push(stripped_line);
    }

    let mut stripped = body.join("\n");
    if trailing_newline {
        stripped.push('\n');
    }
    stripped
}

fn extract_dataview_fields_from_line(
    line: &str,
    field_start_re: &Regex,
) -> (String, Vec<(String, String)>) {
    let matches = field_start_re.find_iter(line).collect::<Vec<_>>();
    if matches.is_empty() {
        return (line.to_string(), Vec::new());
    }

    let bytes = line.as_bytes();
    let mut stripped = String::with_capacity(line.len());
    let mut fields = Vec::new();
    let mut cursor = 0usize;
    let mut idx = 0usize;

    while idx < matches.len() {
        let marker = &matches[idx];
        let start = marker.start();
        if start > 0 && !bytes[start - 1].is_ascii_whitespace() {
            idx += 1;
            continue;
        }

        stripped.push_str(&line[cursor..start]);

        let key_end = marker.end().saturating_sub(2);
        let key = line[start..key_end].trim();

        let mut value_start = marker.end();
        while value_start < line.len() && bytes[value_start].is_ascii_whitespace() {
            value_start += 1;
        }

        let mut value_end = line.len();
        for next in matches.iter().skip(idx + 1) {
            let next_start = next.start();
            if next_start == 0 {
                continue;
            }
            if bytes[next_start - 1].is_ascii_whitespace() {
                value_end = next_start;
                break;
            }
        }

        if !key.is_empty() {
            let value = line[value_start..value_end].trim().to_string();
            fields.push((key.to_string(), value));
        }

        cursor = value_end;
        while cursor < line.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }

        while idx + 1 < matches.len() && matches[idx + 1].start() < cursor {
            idx += 1;
        }
        idx += 1;
    }

    stripped.push_str(&line[cursor..]);
    (stripped.trim_end().to_string(), fields)
}

pub fn extract_dataview_inline_fields(input: &str) -> (String, Vec<(String, String)>) {
    let field_start_re = Regex::new(r"[A-Za-z0-9_-]+::").expect("valid regex");
    let mut body = Vec::new();
    let mut fields = Vec::new();
    let trailing_newline = input.ends_with('\n');

    for line in input.lines() {
        let normalized = line.trim_end_matches('\r');
        let (stripped_line, mut line_fields) =
            extract_dataview_fields_from_line(normalized, &field_start_re);
        if line_fields.is_empty() {
            body.push(normalized.to_string());
            continue;
        }

        fields.append(&mut line_fields);
        if !stripped_line.trim().is_empty() {
            body.push(stripped_line);
        }
    }

    let mut stripped = body.join("\n");
    if trailing_newline {
        stripped.push('\n');
    }
    (stripped, fields)
}

pub fn convert_soft_breaks_to_hard(input: &str) -> String {
    let lines: Vec<&str> = input.split('\n').collect();
    if lines.len() <= 1 {
        return input.to_string();
    }

    let mut out = String::with_capacity(input.len() + 8);
    for (idx, line) in lines.iter().enumerate() {
        out.push_str(line);
        if idx + 1 >= lines.len() {
            continue;
        }

        let next = lines[idx + 1];
        let current_nonempty = !line.trim().is_empty();
        let next_nonempty = !next.trim().is_empty();
        if current_nonempty && next_nonempty {
            out.push_str("  \n");
        } else {
            out.push('\n');
        }
    }
    out
}

pub fn transform_callouts(input: &str) -> String {
    let header_re = Regex::new(r"^>\s*\[!([A-Za-z]+)\]([+-])?\s*(.*)$").expect("valid regex");
    let mut out = Vec::new();
    let lines: Vec<&str> = input.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        if let Some(caps) = header_re.captures(line) {
            let callout_type = caps
                .get(1)
                .map(|m| m.as_str().to_ascii_lowercase())
                .unwrap_or_else(|| "note".to_string());
            let fold = caps.get(2).map(|m| m.as_str());
            let title = caps.get(3).map(|m| m.as_str().trim()).unwrap_or("");

            i += 1;
            let mut content = Vec::new();
            while i < lines.len() && lines[i].trim_start().starts_with('>') {
                let stripped = lines[i]
                    .trim_start()
                    .trim_start_matches('>')
                    .trim_start()
                    .to_string();
                content.push(stripped);
                i += 1;
            }

            let fold_attr = match fold {
                Some("+") => " data-foldable=\"open\"",
                Some("-") => " data-foldable=\"closed\"",
                _ => "",
            };

            out.push(format!(
                "<div class=\"callout callout-{callout_type}\"{fold_attr}>"
            ));
            if fold.is_some() || !title.is_empty() {
                out.push(format!(
                    "<div class=\"callout-title\">{}</div>",
                    html_escape(title)
                ));
            }
            out.push("<div class=\"callout-content\">".to_string());
            out.push(content.join("\n"));
            out.push("</div>".to_string());
            out.push("</div>".to_string());
            continue;
        }

        out.push(line.to_string());
        i += 1;
    }

    out.join("\n")
}

pub fn extract_heading_section(body: &str, heading: &str) -> Option<String> {
    let heading_slug = slugify(heading);
    let heading_re = Regex::new(r"^(#{1,6})\s+(.*)$").expect("valid regex");
    let lines: Vec<&str> = body.lines().collect();

    let mut start_idx = None;
    let mut level = 0usize;

    for (idx, line) in lines.iter().enumerate() {
        if let Some(caps) = heading_re.captures(line) {
            let current_level = caps.get(1).map(|m| m.as_str().len()).unwrap_or(0);
            let title = caps.get(2).map(|m| m.as_str().trim()).unwrap_or("");
            if slugify(title) == heading_slug {
                start_idx = Some(idx + 1);
                level = current_level;
                break;
            }
        }
    }

    let start = start_idx?;
    let mut end = lines.len();
    for (idx, line) in lines.iter().enumerate().skip(start) {
        if let Some(caps) = heading_re.captures(line) {
            let current_level = caps.get(1).map(|m| m.as_str().len()).unwrap_or(7);
            if current_level <= level {
                end = idx;
                break;
            }
        }
    }

    Some(lines[start..end].join("\n"))
}

pub fn parse_wikilink_spec(spec: &str) -> WikiSpec {
    let mut left = spec.trim();
    let mut alias = None;

    if let Some((a, b)) = left.split_once('|') {
        left = a.trim();
        alias = Some(b.trim().to_string());
    }

    let (target, heading) = if let Some((a, b)) = left.split_once('#') {
        (a.trim().to_string(), Some(b.trim().to_string()))
    } else {
        (left.trim().to_string(), None)
    };

    WikiSpec {
        target,
        heading,
        alias,
    }
}

#[derive(Debug, Clone)]
pub struct WikiSpec {
    pub target: String,
    pub heading: Option<String>,
    pub alias: Option<String>,
}

pub fn normalize_note_target(target: &str) -> String {
    let mut t = target.trim().replace('\\', "/");
    if t.ends_with(".md") {
        t.truncate(t.len().saturating_sub(3));
    }
    t
}

pub fn normalize_relative_link(base_note: &Path, target: &str) -> Option<PathBuf> {
    if target.trim().is_empty() {
        return None;
    }

    let base_dir = base_note.parent().unwrap_or_else(|| Path::new(""));
    let mut out = PathBuf::new();

    for comp in base_dir.components() {
        if let Component::Normal(v) = comp {
            out.push(v);
        }
    }

    for comp in Path::new(target).components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(v) => out.push(v),
            Component::RootDir => out.clear(),
            _ => {}
        }
    }

    Some(out)
}

pub fn markdown_link_target(raw: &str) -> Cow<'_, str> {
    let trimmed = raw.trim();
    if trimmed.starts_with('<')
        && let Some(end) = trimmed.find('>')
    {
        return Cow::Owned(trimmed[1..end].to_string());
    }

    if let Some(last) = trimmed.chars().last()
        && (last == '"' || last == '\'')
        && let Some(open_idx) = trimmed[..trimmed.len().saturating_sub(1)].rfind(last)
    {
        let before = &trimmed[..open_idx];
        if before.chars().last().is_some_and(char::is_whitespace) {
            let target = before.trim_end();
            if !target.is_empty() {
                return Cow::Borrowed(target);
            }
        }
    }

    Cow::Borrowed(trimmed)
}

pub fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn is_external_or_anchor(target: &str) -> bool {
    let t = target.trim();
    t.starts_with("http://")
        || t.starts_with("https://")
        || t.starts_with("mailto:")
        || t.starts_with('#')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn frontmatter_split_works() {
        let (fm, body) = split_frontmatter("---\ntitle: A\n---\nHello");
        assert_eq!(fm.as_deref(), Some("title: A"));
        assert_eq!(body, "Hello");
    }

    #[test]
    fn frontmatter_split_supports_crlf_and_eof_closer() {
        let (fm, body) = split_frontmatter("---\r\ntitle: A\r\n---\r\nHello");
        assert_eq!(fm.as_deref(), Some("title: A"));
        assert_eq!(body, "Hello");

        let (fm2, body2) = split_frontmatter("---\ntitle: A\n---");
        assert_eq!(fm2.as_deref(), Some("title: A"));
        assert!(body2.is_empty());
    }

    #[test]
    fn slugify_is_stable() {
        assert_eq!(slugify("My Note 2026"), "my-note-2026");
        assert_eq!(slugify("A___B"), "a-b");
    }

    #[test]
    fn url_path_generation() {
        let path = Path::new("Folder/My Note.md");
        assert_eq!(url_path_from_relative(path), "/folder/my-note/");
    }

    #[test]
    fn callout_transform() {
        let raw = "> [!warning]+ Be careful\n> one\n> two\nplain";
        let out = transform_callouts(raw);
        assert!(out.contains("callout-warning"));
        assert!(out.contains("data-foldable=\"open\""));
        assert!(out.contains("one\ntwo"));
    }

    #[test]
    fn foldable_callout_without_title_keeps_title_container() {
        let raw = "> [!tip]-\n> hidden";
        let out = transform_callouts(raw);
        assert!(out.contains("data-foldable=\"closed\""));
        assert!(out.contains("<div class=\"callout-title\"></div>"));
        assert!(out.contains("hidden"));
    }

    #[test]
    fn heading_section_extraction() {
        let body = "# A\ntext\n## B\none\n## C\ntwo";
        let section = extract_heading_section(body, "B").expect("section");
        assert_eq!(section.trim(), "one");
    }

    #[test]
    fn wikilink_parser() {
        let spec = parse_wikilink_spec("Target#Heading|Alias");
        assert_eq!(spec.target, "Target");
        assert_eq!(spec.heading.as_deref(), Some("Heading"));
        assert_eq!(spec.alias.as_deref(), Some("Alias"));
    }

    #[test]
    fn relative_link_normalization() {
        let base = Path::new("folder/note.md");
        let rel = normalize_relative_link(base, "../img/a.png").expect("normalized path");
        assert_eq!(rel, PathBuf::from("img/a.png"));
    }

    #[test]
    fn dataview_extraction_strips_and_collects_fields() {
        let (body, fields) = extract_dataview_inline_fields("a:: 1\nBody\nstatus:: open\n");
        assert_eq!(body, "Body\n");
        assert_eq!(
            fields,
            vec![
                ("a".to_string(), "1".to_string()),
                ("status".to_string(), "open".to_string())
            ]
        );
    }

    #[test]
    fn dataview_inline_extraction_supports_non_line_start_fields() {
        let (body, fields) = extract_dataview_inline_fields("Task status:: open\nBody\n");
        assert_eq!(body, "Task\nBody\n");
        assert_eq!(fields, vec![("status".to_string(), "open".to_string())]);
    }

    #[test]
    fn dataview_strip_supports_non_line_start_fields() {
        let stripped = strip_dataview_inline_fields("Task status:: open\nBody\n");
        assert_eq!(stripped, "Task\nBody\n");
    }

    #[test]
    fn soft_break_conversion_makes_hard_breaks() {
        let converted = convert_soft_breaks_to_hard("first\nsecond\n\nthird\nfourth");
        assert_eq!(converted, "first  \nsecond\n\nthird  \nfourth");
    }
}
