#[derive(Clone)]
pub enum Selection {
    All,
    Some(Vec<String>)
}

impl Selection {
    pub fn is_empty(&self) -> bool {
        match self {
            Selection::Some(list) => list.is_empty(),
            Selection::All => false,
        }
    }

    pub fn get_some_selected_columns(&self) -> Vec<String> {
        match &self {
            Selection::Some(values) => values.clone(),
            Selection::All => Vec::new(),
        }
    }
}