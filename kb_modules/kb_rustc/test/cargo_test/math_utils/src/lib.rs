/// A tiny math helper library.

/// Adds two integers.
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn add_works() {
        assert_eq!(add(2, 3), 5);
    }
}
