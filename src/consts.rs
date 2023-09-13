include!(concat!(env!("OUT_DIR"), "/consts-codegen.rs"));

pub(crate) static SLEEP_ACTIVITY: &str = "sleep (night)";

pub(crate) static LAST_MOMENT_OF_THE_DAY: i64 = 1000 * 1000 * 1000 * 60 * 60 * 23
    + (1000 * 1000 * 1000 * 60 * 59)
    + (1000 * 1000 * 1000 * 59)
    + (1000 * 1000 * 999);

pub(crate) static FACTOR_TAG_RE: &str = r"^\s*<\s*(?P<factor_type>[A-z]+)\s*_\s*(?P<factor_name>[A-z]+)\s*(?P<factor_scale>[+-]?\d+(.\d+)?)?\s*>\s*(?P<description>([^\s]|\s)+)?\s*$";
