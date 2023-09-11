use polars::prelude::LiteralValue;

include!(concat!(env!("OUT_DIR"), "/consts-codegen.rs"));

pub(crate) static MIN_ACTIVITY_SAMPLE_THRESHOLD: u8 = 3;

pub(crate) static SLEEP_ACTIVITY: &str = "sleep (night)";

pub(crate) static LAST_MOMENT_OF_THE_DAY: i64 = 1000 * 1000 * 1000 * 60 * 60 * 23
    + (1000 * 1000 * 1000 * 60 * 59)
    + (1000 * 1000 * 1000 * 59)
    + (1000 * 1000 * 999);

pub(crate) static LAST_MOMENT_OF_THE_DAY_LITERAL: LiteralValue =
    LiteralValue::Time(LAST_MOMENT_OF_THE_DAY);

pub(crate) static FACTOR_TAG_RE: &str = r"^\s*<\s*(?P<factor_type>[A-z]+)\s*_\s*(?P<factor_name>[A-z]+)\s*(?P<factor_scale>[+-]?\d+(.\d+)?)?\s*>\s*(?P<description>([^\s]|\s)+)?\s*$";

impl Factor {
    pub fn name(&self) -> &'static str {
        match self {
            Factor::SingleValue { name, .. } | Factor::MultipleValue { name, .. } => name,
        }
    }
    pub fn types(&self) -> &'static [&'static FactorType] {
        match self {
            Factor::SingleValue { types, .. } | Factor::MultipleValue { types, .. } => types,
        }
    }
    pub fn tag(&self) -> &'static str {
        match self {
            Factor::SingleValue { tag, .. } | Factor::MultipleValue { tag, .. } => tag,
        }
    }
}

impl FactorType {
    pub fn name(&self) -> Option<&'static str> {
        match self {
            FactorType::Taxonomy { name, .. } | FactorType::Scale { name, .. } => *name,
        }
    }
    pub fn tag(&self) -> &'static str {
        match self {
            FactorType::Taxonomy { tag, .. } | FactorType::Scale { tag, .. } => tag,
        }
    }
}

impl Mood {
    pub fn mood_data(&'static self) -> &'static MoodData {
        match self {
            Mood::ReallyGood(data)
            | Mood::ReallyGoodMinus(data)
            | Mood::Good(data)
            | Mood::GoodMinus(data)
            | Mood::Medium(data)
            | Mood::MediumMinus(data)
            | Mood::NotSoGood(data)
            | Mood::NotSoGoodMinus(data)
            | Mood::Bad(data)
            | Mood::BadMinus(data)
            | Mood::Awful(data)
            | Mood::AwfulMinus(data) => data,
        }
    }
}
