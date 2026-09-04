//! Protocol-edge mapping of multimodal [`ContentBlock`]s to each provider's
//! wire format. These are pure functions: they take the message blocks plus the
//! target model's capabilities and emit provider-specific JSON.
//!
//! Media a model cannot accept is *downgraded* — the block is dropped and a
//! short text note is appended so the model knows an attachment was omitted and
//! can ask for its content as text rather than treating the attachment as
//! empty. We never send a block the model's protocol does not define.

use cog_core::ContentBlock;
use serde_json::{json, Value};

use crate::model::Model;

/// Coarse modality of a MIME type, used for capability gating and the
/// downgrade note.
fn modality(mime: &str) -> &'static str {
    let m = mime.to_ascii_lowercase();
    if m.starts_with("image/") {
        "image"
    } else if m.starts_with("audio/") {
        "audio"
    } else if m.starts_with("video/") {
        "video"
    } else if m == "application/pdf" {
        "pdf"
    } else {
        "other"
    }
}

/// Concatenate all text blocks (thinking and media excluded).
pub(crate) fn text_of(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| b.as_text())
        .collect::<Vec<_>>()
        .join("")
}

/// Note appended when one or more media blocks were dropped for lack of model
/// support, so the model can request the content as text instead of assuming
/// nothing was attached.
fn downgrade_note(dropped: &[&'static str]) -> String {
    let mut kinds: Vec<&'static str> = dropped.to_vec();
    kinds.dedup();
    format!(
        "\n\n[Note: {} media attachment(s) of type {} could not be processed by this model. \
         If their content is needed to answer, ask the user to provide it as text.]",
        dropped.len(),
        kinds.join(", ")
    )
}

/// OpenAI Chat Completions: a plain string for text-only messages, or an array
/// of parts (`text`, `image_url` data-URI, `input_audio`) when media is present.
/// Images require a vision model; audio only OpenAI audio models; video and PDF
/// are not supported on this wire shape and are downgraded.
pub(crate) fn openai_content(blocks: &[ContentBlock], model: &Model) -> Value {
    let mut parts: Vec<Value> = Vec::new();
    let mut dropped: Vec<&'static str> = Vec::new();
    let mut has_media = false;

    for b in blocks {
        match b {
            ContentBlock::Text { text, .. } => {
                parts.push(json!({"type": "text", "text": text}));
            }
            ContentBlock::Media { data, mime_type } => {
                let kind = modality(mime_type);
                let supported = match kind {
                    "image" => model.supports_vision,
                    "audio" => {
                        model.supports_vision && model.id.to_ascii_lowercase().contains("audio")
                    }
                    _ => false,
                };
                if !supported {
                    dropped.push(kind);
                    continue;
                }
                has_media = true;
                match kind {
                    "image" => parts.push(json!({
                        "type": "image_url",
                        "image_url": {"url": format!("data:{};base64,{}", mime_type, data)}
                    })),
                    "audio" => parts.push(json!({
                        "type": "input_audio",
                        "input_audio": {"data": data, "format": audio_format(mime_type)}
                    })),
                    _ => unreachable!(),
                }
            }
            _ => {}
        }
    }

    if !has_media {
        // No media survived gating: keep the plain-string wire shape, with the
        // downgrade note (if any) folded into the text.
        let mut s = text_of(blocks);
        if !dropped.is_empty() {
            s.push_str(&downgrade_note(&dropped));
        }
        return json!(s);
    }
    if !dropped.is_empty() {
        parts.push(json!({"type": "text", "text": downgrade_note(&dropped)}));
    }
    json!(parts)
}

/// Anthropic Messages: content array of `text`, `image` (base64 source), and
/// `document` (PDF) blocks. Audio/video are not supported and are downgraded.
pub(crate) fn anthropic_content(blocks: &[ContentBlock], model: &Model) -> Value {
    let mut out: Vec<Value> = Vec::new();
    let mut dropped: Vec<&'static str> = Vec::new();
    let mut has_media = false;

    for b in blocks {
        match b {
            ContentBlock::Text { text, .. } => {
                out.push(json!({"type": "text", "text": text}));
            }
            ContentBlock::Media { data, mime_type } => {
                let kind = modality(mime_type);
                if !model.supports_vision || !matches!(kind, "image" | "pdf") {
                    dropped.push(kind);
                    continue;
                }
                has_media = true;
                let block_type = if kind == "pdf" { "document" } else { "image" };
                out.push(json!({
                    "type": block_type,
                    "source": {
                        "type": "base64",
                        "media_type": mime_type,
                        "data": data,
                    }
                }));
            }
            _ => {}
        }
    }

    if !has_media {
        // No media survived gating: keep the plain-string wire shape, folding
        // any downgrade note into the text.
        let mut s = text_of(blocks);
        if !dropped.is_empty() {
            s.push_str(&downgrade_note(&dropped));
        }
        if s.is_empty() {
            return json!([{"type": "text", "text": ""}]);
        }
        return json!(s);
    }
    if !dropped.is_empty() {
        out.push(json!({"type": "text", "text": downgrade_note(&dropped)}));
    }
    if out.is_empty() {
        out.push(json!({"type": "text", "text": ""}));
    }
    json!(out)
}

/// Google Generative Language: `parts` of `text` and `inline_data`
/// (base64 + MIME). Gemini multimodal models accept image, audio, video, and
/// PDF inline data; non-vision models downgrade all media.
pub(crate) fn gemini_parts(blocks: &[ContentBlock], model: &Model) -> Vec<Value> {
    let mut parts: Vec<Value> = Vec::new();
    let mut dropped: Vec<&'static str> = Vec::new();

    for b in blocks {
        match b {
            ContentBlock::Text { text, .. } => {
                parts.push(json!({"text": text}));
            }
            ContentBlock::Media { data, mime_type } => {
                let kind = modality(mime_type);
                if !model.supports_vision || matches!(kind, "other") {
                    dropped.push(kind);
                    continue;
                }
                parts.push(json!({
                    "inline_data": {"mime_type": mime_type, "data": data}
                }));
            }
            _ => {}
        }
    }

    if !dropped.is_empty() {
        parts.push(json!({"text": downgrade_note(&dropped)}));
    }
    if parts.is_empty() {
        parts.push(json!({"text": ""}));
    }
    parts
}

/// Ollama chat: a single text string plus a separate top-level `images` array
/// of raw base64 strings (images only). Audio/video/PDF are downgraded.
pub(crate) fn ollama_content(blocks: &[ContentBlock], model: &Model) -> (String, Vec<String>) {
    let mut text = String::new();
    let mut images: Vec<String> = Vec::new();
    let mut dropped: Vec<&'static str> = Vec::new();

    for b in blocks {
        match b {
            ContentBlock::Text { text: t, .. } => text.push_str(t),
            ContentBlock::Media { data, mime_type } => {
                let kind = modality(mime_type);
                if kind == "image" && model.supports_vision {
                    images.push(data.clone());
                } else {
                    dropped.push(kind);
                }
            }
            _ => {}
        }
    }

    if !dropped.is_empty() {
        text.push_str(&downgrade_note(&dropped));
    }
    (text, images)
}

/// Map an audio MIME type to OpenAI's `input_audio.format` token.
fn audio_format(mime: &str) -> &'static str {
    match mime.to_ascii_lowercase().as_str() {
        "audio/wav" | "audio/x-wav" | "audio/wave" => "wav",
        "audio/mp3" | "audio/mpeg" => "mp3",
        _ => "mp3",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ApiType, Model, ModelCost, Provider};
    use std::collections::HashMap;

    fn model(vision: bool, id: &str, api: ApiType, provider: Provider) -> Model {
        Model {
            id: id.into(),
            name: id.into(),
            api,
            provider,
            base_url: "http://localhost".into(),
            context_window: 1000,
            max_tokens: 100,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: vision,
            supports_reasoning: false,
            cost: ModelCost::default(),
            headers: HashMap::new(),
        }
    }

    fn blocks() -> Vec<ContentBlock> {
        vec![
            ContentBlock::text("describe this"),
            ContentBlock::media("BASE64IMG", "image/png"),
            ContentBlock::media("BASE64AUDIO", "audio/mpeg"),
            ContentBlock::media("BASE64VIDEO", "video/mp4"),
        ]
    }

    #[test]
    fn openai_vision_packs_image_and_audio_model_packs_audio() {
        let vision = model(true, "gpt-4o", ApiType::OpenAICompletions, Provider::OpenAI);
        let v = openai_content(&blocks(), &vision);
        let parts = v.as_array().unwrap();
        assert!(parts.iter().any(|p| p["type"] == "image_url"
            && p["image_url"]["url"] == "data:image/png;base64,BASE64IMG"));
        // Non-audio vision model: audio/video dropped with a note, no input_audio.
        assert!(!parts.iter().any(|p| p["type"] == "input_audio"));
        assert!(parts.iter().any(|p| p["type"] == "text"
            && p["text"]
                .as_str()
                .unwrap()
                .contains("could not be processed")));

        let audio = model(
            true,
            "gpt-4o-audio-preview",
            ApiType::OpenAICompletions,
            Provider::OpenAI,
        );
        let a = openai_content(&blocks(), &audio);
        let aparts = a.as_array().unwrap();
        assert!(aparts
            .iter()
            .any(|p| p["type"] == "input_audio" && p["input_audio"]["data"] == "BASE64AUDIO"));
    }

    #[test]
    fn openai_text_only_model_downgrades_media_to_string() {
        let no_vision = model(
            false,
            "gpt-3.5",
            ApiType::OpenAICompletions,
            Provider::OpenAI,
        );
        let v = openai_content(&blocks(), &no_vision);
        // No array parts: content stays a plain string, no image_url leaks.
        assert!(v.is_string());
        let s = v.as_str().unwrap();
        assert!(s.contains("describe this"));
        assert!(s.contains("could not be processed"));
    }

    #[test]
    fn anthropic_packs_image_and_pdf_drops_audio_video() {
        let vision = model(
            true,
            "claude",
            ApiType::AnthropicMessages,
            Provider::Anthropic,
        );
        let mut blk = blocks();
        blk.push(ContentBlock::media("BASE64PDF", "application/pdf"));
        let v = anthropic_content(&blk, &vision);
        let arr = v.as_array().unwrap();
        assert!(arr
            .iter()
            .any(|p| p["type"] == "image" && p["source"]["data"] == "BASE64IMG"));
        assert!(arr
            .iter()
            .any(|p| p["type"] == "document" && p["source"]["media_type"] == "application/pdf"));
        // Audio/video are not Anthropic-supported: downgraded, never sent.
        assert!(!arr
            .iter()
            .any(|p| p["source"]["media_type"] == "audio/mpeg"));
        assert!(!arr.iter().any(|p| p["source"]["media_type"] == "video/mp4"));
    }

    #[test]
    fn gemini_inline_data_packs_image_audio_video() {
        let vision = model(
            true,
            "gemini",
            ApiType::GoogleGenerativeAI,
            Provider::Google,
        );
        let parts = gemini_parts(&blocks(), &vision);
        for (mime, data) in [
            ("image/png", "BASE64IMG"),
            ("audio/mpeg", "BASE64AUDIO"),
            ("video/mp4", "BASE64VIDEO"),
        ] {
            assert!(
                parts
                    .iter()
                    .any(|p| p["inline_data"]["mime_type"] == mime
                        && p["inline_data"]["data"] == data)
            );
        }
    }

    #[test]
    fn gemini_non_vision_downgrades() {
        let no_vision = model(
            false,
            "gemini-text",
            ApiType::GoogleGenerativeAI,
            Provider::Google,
        );
        let parts = gemini_parts(&blocks(), &no_vision);
        assert!(!parts.iter().any(|p| p.get("inline_data").is_some()));
        assert!(parts.iter().any(|p| p["text"]
            .as_str()
            .unwrap_or("")
            .contains("could not be processed")));
    }

    #[test]
    fn ollama_images_only_when_vision() {
        let vision = model(true, "llava", ApiType::OllamaChat, Provider::Ollama);
        let (text, imgs) = ollama_content(&blocks(), &vision);
        assert_eq!(imgs, vec!["BASE64IMG".to_string()]);
        // Audio/video downgraded into a text note.
        assert!(text.contains("could not be processed"));

        let no_vision = model(false, "llama", ApiType::OllamaChat, Provider::Ollama);
        let (text2, imgs2) = ollama_content(&blocks(), &no_vision);
        assert!(imgs2.is_empty());
        assert!(text2.contains("could not be processed"));
    }

    #[test]
    fn text_only_message_keeps_plain_string_shape() {
        let vision = model(true, "gpt-4o", ApiType::OpenAICompletions, Provider::OpenAI);
        let v = openai_content(&[ContentBlock::text("hello")], &vision);
        assert_eq!(v, json!("hello"));
        let a = anthropic_content(&[ContentBlock::text("hello")], &vision);
        assert_eq!(a, json!("hello"));
    }
}
