use std::borrow::Cow;
use polars::export::num::FromPrimitive;
use polars::prelude::*;
use crate::{FactorColumn};

pub fn convert_time_units(v: i64, tu_l: TimeUnit, tu_r: TimeUnit) -> i64 {
    match (tu_l, tu_r) {
        (TimeUnit::Nanoseconds, TimeUnit::Microseconds) => v / 1_000,
        (TimeUnit::Nanoseconds, TimeUnit::Milliseconds) => v / 1_000_000,
        (TimeUnit::Microseconds, TimeUnit::Nanoseconds) => v * 1_000,
        (TimeUnit::Microseconds, TimeUnit::Milliseconds) => v / 1_000,
        (TimeUnit::Milliseconds, TimeUnit::Microseconds) => v * 1_000,
        (TimeUnit::Milliseconds, TimeUnit::Nanoseconds) => v * 1_000_000,
        _ => v,
    }
}


#[derive(Debug, Clone, PartialEq, PartialOrd, Default)]
pub struct ValueRange {
    pub value: f32,
    pub min: Option<f32>,
    pub max: Option<f32>,
}

pub fn map_sleep_duration_value_to_value_range(sleep: f32) -> Option<ValueRange> {
    match i32::from_f32(sleep)? {
        190 => Some(ValueRange {
            value: 14.0,
            min: Some(13.0),
            max: None,
        }),
        150 => Some(ValueRange {
            value: 11.0,
            min: Some(10.0),
            max: Some(13.0),
        }),
        100 => Some(ValueRange {
            value: 8.0,
            min: Some(7.0),
            max: Some(10.0),
        }),
        65 => Some(ValueRange {
            value: 5.0,
            min: Some(4.0),
            max: Some(7.0),
        }),
        33 => Some(ValueRange {
            value: 2.5,
            min: Some(1.0),
            max: Some(4.0),
        }),
        0 => Some(ValueRange {
            value: 0.0,
            min: Some(0.0),
            max: Some(1.0),
        }),
        _ => None,
    }
}

pub fn get_factor_column_name(factors: Vec<FactorColumn>, factor_tag: &str, factor_type_tag: &str) -> Option<Cow<'_, str>> {
    factors
        .iter()
        .find(|factor_column| {
            *factor_column.factor.tag() == factor_tag && *factor_column.factor_type.tag() == factor_type_tag
        })
        .map(|factor_column| factor_column.column_name.clone())
}