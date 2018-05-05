#[derive(Debug, Clone)]
pub struct Range {
    pub low: usize,
    pub high: usize,
}

impl Range {
    pub fn new(low: usize, high: usize) -> Range {
        Range {
            low: low,
            high: high,
        }
    }
}
