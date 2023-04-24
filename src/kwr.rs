#[derive(Debug, Clone, Copy)]
pub enum Keywords {
    Lt,
    Gt,
    Eq,
    Neq,
    Like,
    Prop,
    Or,
    Offset,
    Limit,
    Set,
    Unknown
}

impl From<&str> for Keywords {
    fn from(value: &str) -> Self {
        match value {
            "$lt" => Keywords::Lt,
            "$gt" => Keywords::Gt,
            "$eq" => Keywords::Eq,
            "$neq" => Keywords::Neq,
            "$like" => Keywords::Like,
            "$prop" => Keywords::Prop,
            "$or" => Keywords::Or,
            "$offset" => Keywords::Offset,
            "$limit" => Keywords::Limit,
            "$set" => Keywords::Set,
            _=> Keywords::Unknown
        }
    }
}

impl Keywords {
    pub fn as_str(&self) -> &str {
        match self {
            Keywords::Lt => "$lt",
            Keywords::Gt => "$gt",
            Keywords::Eq => "$eq",
            Keywords::Neq => "$neq",
            Keywords::Like => "$like",
            Keywords::Prop => "$prop",
            Keywords::Or => "$or",
            Keywords::Offset => "$offset",
            Keywords::Limit => "$limit",
            Keywords::Set => "$set",
            _ => ""
        }
    }

    pub fn object_based(keyword: &str) -> bool {
        vec![
            Keywords::Lt.as_str(),
            Keywords::Gt.as_str(),
            Keywords::Eq.as_str(),
            Keywords::Neq.as_str(),
            Keywords::Like.as_str(),
            Keywords::Prop.as_str(),
            Keywords::Set.as_str()
        ].contains(&keyword)
    }

    pub fn array_based(keyword: &str) -> bool {
        vec![
            Keywords::Or.as_str()
        ].contains(&keyword)
    }

    pub fn alphn_based(keyword: &str) -> bool {
        todo!()
    }
}