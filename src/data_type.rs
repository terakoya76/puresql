#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Bool,
    Char(u8),
}

#[cfg(test)]
mod tests {}
