use std::str::FromStr;
#[derive(PartialEq, Eq, Debug)]
pub enum Mode {
    STATIC,
    DYNAMIC,
}

impl FromStr for Mode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "static" => Ok(Mode::STATIC),
            "dynamic" => Ok(Mode::DYNAMIC),
            _ => Ok(Mode::STATIC),
        }
    }
}