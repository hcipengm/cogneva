/// Remove or replace Unicode surrogate pairs (U+D800-U+DFFF range) that are invalid in UTF-8.
pub fn sanitize_surrogates(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if (0xD800..=0xDFFF).contains(&u32::from(c)) {
                '\u{FFFD}' // Unicode replacement character
            } else {
                c
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_surrogates() {
        assert_eq!(sanitize_surrogates("hello"), "hello");
    }
}
