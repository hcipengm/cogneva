//! How much of a wiki page reaches a model.
//!
//! A wiki page is stored and read whole: its content *is* the document, and a
//! backend that returned a shortened one would be reporting a document it does
//! not hold. So the bound does not belong in a backend. It belongs at the two
//! places where the text stops being a document and becomes prompt text, and
//! those two have to agree — two caps that both mean "one page in a prompt"
//! are one cap with two spellings, and the second one to change is the one
//! nobody reads.

/// Characters of a single wiki page that may be handed to a model.
///
/// A page has no size bound of its own (it is whatever whoever wrote it put
/// there), so a retrieval that returns several of them hands a model an amount
/// of text that grows with the wiki rather than with the question. This is the
/// per-page ceiling that keeps that from being the case.
///
/// The number is the size of one page's worth of context for a model. The pass
/// that used to write pages held the same number; it is gone, and the quantity
/// it bounded is not, so the ceiling stays where it was rather than being
/// chosen again.
// cap: bound — 一页交给模型的字符上限：页本身没有大小界，读回来几页就得在这里截
pub const WIKI_PAGE_PROMPT_CHARS: usize = 2_000;
