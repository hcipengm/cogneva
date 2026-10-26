//! Filling the words a query template names into the statement it becomes.
//!
//! A closed set is declared once, by the enum whose `as_str` decides each of its
//! words. SQL cannot call that method, so every query that predicates on one of
//! those words has to receive the spelling from outside -- and the two ways to
//! get it wrong are both quiet:
//!
//! - Spelling the word in the query is a second copy. The two agree until one is
//!   renamed, and then the predicate keeps matching the old spelling. A query
//!   that matches no rows answers exactly like a table with nothing in it, so
//!   "nothing is outstanding", "nothing was retired" and "nothing is firing" all
//!   read as ordinary states, and no layer below the reader ever complains.
//! - Filling a slot that the template does not spell, or forgetting to fill one
//!   it does, leaves `{...}` in the statement: the database refuses to parse it
//!   at the moment the query is first needed, which for a ledger write is the
//!   moment it has something to say.
//!
//! So templates name a slot per value -- `{state:landed}` -- and callers hand in
//! the enum's own [`ALL`](crate::ChangeState::ALL) mapped through `as_str`. A
//! variant added to the set is fillable without an edit at the call site, and a
//! slot nobody fills is a visible `{` rather than a silently wrong predicate.
//!
//! One function rather than one per store: a store that fixes a bug here must
//! not leave the other copies broken.
pub fn fill_slots(template: &str, slots: &[(&str, &str)]) -> String {
    let mut sql = template.to_string();
    for (prefix, word) in slots {
        sql = sql.replace(&format!("{{{prefix}:{word}}}"), word);
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_named_slot_is_replaced() {
        let sql = fill_slots(
            "WHERE (state IS NULL OR state = '{state:unenforced}') \
               AND check_kind = '{kind:judgement}' \
               AND state = '{state:unenforced}'",
            &[("state", "unenforced"), ("kind", "judgement")],
        );
        assert_eq!(
            sql,
            "WHERE (state IS NULL OR state = 'unenforced') \
               AND check_kind = 'judgement' \
               AND state = 'unenforced'"
        );
        assert!(!sql.contains('{'));
    }

    /// A slot the caller did not offer stays in the text, which is the point:
    /// the statement then fails to parse loudly instead of matching nothing.
    #[test]
    fn a_slot_nobody_fills_is_left_in_the_statement() {
        let sql = fill_slots("WHERE state = '{state:retired}'", &[("state", "landed")]);
        assert_eq!(sql, "WHERE state = '{state:retired}'");
    }

    /// The prefix is part of the slot, so two vocabularies in one statement do
    /// not overwrite each other's words.
    #[test]
    fn a_prefix_only_fills_its_own_slots() {
        let sql = fill_slots(
            "state = '{state:landed}' AND to_state = '{state:landed}'",
            &[("state", "landed")],
        );
        assert_eq!(sql, "state = 'landed' AND to_state = 'landed'");
        let other = fill_slots(
            "WHERE blocked_reason = '{blocked:pool_unavailable}'",
            &[("state", "landed")],
        );
        assert!(other.contains("{blocked:pool_unavailable}"));
    }
}
