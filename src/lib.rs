use crate::consts::{
    TimeBlock, FACTOR_TAG_RE, LAST_MOMENT_OF_THE_DAY, SLEEP_ACTIVITY, TIME_OF_DAY_INTERVALS,
};
use polars::export::rayon::prelude::*;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use std::collections::HashMap;
use std::ops::Div;

#[macro_use]
extern crate enum_access;

pub(crate) mod consts;

pub use crate::consts::{
    Factor, FactorType, Mood, MoodData, FACTORS, FACTOR_TYPES, MOOD_2_MOOD_ENUM,
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error(transparent)]
    Polars(#[from] PolarsError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("unknown processing error")]
    Unknown,
}

fn factor_column_name(factor: &Factor, factor_type: &FactorType) -> String {
    let tag = factor.tag();
    let factor_type_tag = factor_type.tag();

    let mut s = format!("factor_{tag}_{factor_type_tag}");
    s.truncate(s.trim_end().len());
    s
}

#[derive(Clone)]
pub struct ProcessedData {
    pub dataframe: LazyFrame,
    pub factors: HashMap<String, Vec<String>>,
    pub activities: Vec<String>,
}

pub fn process(lf1: LazyFrame) -> Result<ProcessedData, ProcessError> {
    let lf1 = lf1
        .with_comm_subexpr_elim(true)
        .with_columns([
            // activities
            col("activities")
                .str()
                .split("|")
                .list()
                .eval(col("").str().strip(None).filter(col("").neq(lit(""))), true)
                .list()
                .unique(),
        ])
        .with_columns([
            // activities/factors
            col("activities")
                .list()
                .eval(
                    col("").filter(
                        col("")
                            .str()
                            .starts_with(lit("<"))
                            .and(col("").str().contains(lit(FACTOR_TAG_RE), false)),
                    ),
                    true,
                )
                .list()
                .eval(
                    col("")
                        .str()
                        .extract_groups(FACTOR_TAG_RE)
                        .expect("Regex is valid"),
                    true,
                )
                .alias("matched_factors"),
            col("activities").list().eval(
                col("").filter(
                    col("")
                        .str()
                        .starts_with(lit("<"))
                        .not()
                        .and(col("").str().contains(lit(FACTOR_TAG_RE), false).not()),
                ),
                true,
            ),
        ])
        .cache();

    let lf_for_activities = lf1.clone();

    let lf1 = lf1
        .with_columns(
            FACTORS
                .into_iter()
                .flat_map(|(&factor_tag, factor)| {
                    factor.types().iter().map(move |&factor_type| {
                        let factor_type_tag = *factor_type.tag();

                        let list_eval = match factor_type {
                            FactorType::Scale { .. } => col("")
                                .struct_()
                                .field_by_name("factor_scale")
                                .cast(DataType::Float32),
                            FactorType::Taxonomy { .. } => {
                                col("").struct_().field_by_name("description")
                            }
                        };

                        let mut new_col = col("matched_factors").list().eval(
                            list_eval.filter(
                                (col("")
                                    .struct_()
                                    .field_by_name("factor_type")
                                    .eq(lit(factor_type_tag)))
                                .and(
                                    col("")
                                        .struct_()
                                        .field_by_name("factor_name")
                                        .eq(lit(factor_tag)),
                                ),
                            ),
                            true,
                        );

                        new_col = match factor {
                            Factor::SingleValue { .. } => new_col.list().first(),
                            Factor::MultipleValue { .. } => new_col,
                        };

                        new_col = if let FactorType::Taxonomy { .. } = factor_type {
                            match factor {
                                Factor::SingleValue { .. } => {
                                    new_col.cast(DataType::Categorical(None))
                                }
                                Factor::MultipleValue { .. } => new_col
                                    .cast(DataType::List(Box::new(DataType::Categorical(None)))),
                            }
                        } else {
                            new_col
                        };

                        new_col.alias(&factor_column_name(factor, factor_type))
                    })
                })
                .collect::<Vec<_>>(),
        )
        .cache();

    let lf_for_factors = lf1.clone();

    let lf1 = lf1
        .with_columns([
            col("time").str().to_time(StrptimeOptions {
                format: Some("%R".to_string()),
                ..Default::default()
            }),
            col("full_date").str().to_date(StrptimeOptions {
                format: Some("%F".to_string()),
                ..Default::default()
            }),
            col("mood").cast(DataType::Categorical(None)),
            //
            when(col("activities").list().contains(lit(SLEEP_ACTIVITY)))
                .then(lit(true))
                .otherwise(lit(false))
                .alias("is_sleep_entry"),
            col("activities").list().lengths().alias("activities_count"),
            col("activities").shift(-1).alias("activities_previous"),
            col("activities").shift(1).alias("activities_next"),
        ])
        .with_columns([
            // date stuff
            (col("full_date").cast(DataType::Utf8) + lit(" ") + col("time").cast(DataType::Utf8))
                .str()
                .to_datetime(
                    Some(TimeUnit::Milliseconds),
                    None,
                    StrptimeOptions {
                        format: Some("%F %T".to_string()),
                        cache: false,
                        ..Default::default()
                    },
                )
                .alias("full_datetime"),
            col("full_date")
                .dt()
                .strftime("%j")
                .cast(DataType::UInt16)
                .alias("day_of_year"),
            col("time")
                .dt()
                .strftime("%H.%M")
                .cast(DataType::Float32)
                .alias("time_numeric"),
            // mood
            col("mood")
                .map(
                    |v| {
                        Ok(Some(
                            v.categorical()
                                .expect("series was not an categorical dtype")
                                .iter_str()
                                .flat_map(|val| {
                                    val.map(|v| {
                                        MOOD_2_MOOD_ENUM.get(v).map(|e| e.mood_data().level)
                                    })
                                })
                                .collect(),
                        ))
                    },
                    GetOutput::from_type(DataType::UInt8),
                )
                .alias("mood_level"),
            col("activities").cast(DataType::List(Box::new(DataType::Categorical(None)))),
            col("activities_previous").cast(DataType::List(Box::new(DataType::Categorical(None)))),
            col("activities_next").cast(DataType::List(Box::new(DataType::Categorical(None)))),
            col("activities")
                .list()
                .intersection(col("activities_previous"))
                .list()
                .unique()
                .cast(DataType::List(Box::new(DataType::Categorical(None))))
                .alias("common_activities_with_previous"),
            col("activities")
                .list()
                .intersection(col("activities_next"))
                .list()
                .unique()
                .cast(DataType::List(Box::new(DataType::Categorical(None))))
                .alias("common_activities_with_next"),
            col("activities")
                .list()
                .difference(col("activities_previous"))
                .list()
                .unique()
                .cast(DataType::List(Box::new(DataType::Categorical(None))))
                .alias("diff_activities_with_previous"),
            col("activities")
                .list()
                .difference(col("activities_next"))
                .list()
                .unique()
                .cast(DataType::List(Box::new(DataType::Categorical(None))))
                .alias("diff_activities_with_next"),
        ])
        .with_columns({
            let mood_improvement_col = when(col("is_sleep_entry"))
                .then(lit(0i16))
                .otherwise(col("mood_level").diff(1, NullBehavior::Ignore))
                .cast(DataType::Int16);

            TIME_OF_DAY_INTERVALS
                .into_iter()
                .map(|(&name, &block)| {
                    match block {
                        TimeBlock::SameDay { from, to, .. } => (col("time_numeric")
                            .gt_eq(lit(from)))
                        .and(col("time_numeric").lt(lit(to))),
                        TimeBlock::AcrossDays { before, after, .. } => (col("time_numeric")
                            .gt_eq(lit(after)))
                        .or(col("time_numeric").lt(lit(before))),
                    }
                    .alias(name)
                })
                .chain([
                    TIME_OF_DAY_INTERVALS
                        .into_iter()
                        .fold(NULL.lit(), |a, (&name, &block)| {
                            when(match block {
                                TimeBlock::SameDay { from, to, .. } => (col("time_numeric")
                                    .gt_eq(lit(from)))
                                .and(col("time_numeric").lt(lit(to))),
                                TimeBlock::AcrossDays { before, after, .. } => {
                                    (col("time_numeric").gt_eq(lit(after)))
                                        .or(col("time_numeric").lt(lit(before)))
                                }
                            })
                            .then(lit(name.replace("Is", "")))
                            .otherwise(a)
                        })
                        .alias("time_of_day"),
                    // logical date
                    when(col("is_sleep_entry"))
                        .then(col("full_date"))
                        .otherwise(NULL.lit())
                        .alias("logical_date_stage_1"),
                    // mood level improvement
                    mood_improvement_col
                        .clone()
                        .shift(1)
                        .alias("mood_level_improvement_previous_raw"),
                    mood_improvement_col
                        .clone()
                        .alias("mood_level_improvement_raw"),
                    mood_improvement_col
                        .clone()
                        .shift(1)
                        .alias("mood_level_improvement_next_raw"),
                    col("mood_level").shift(-1).alias("mood_level_previous"),
                    //
                    col("common_activities_with_previous")
                        .list()
                        .lengths()
                        .alias("common_activities_with_previous_count"),
                    col("common_activities_with_next")
                        .list()
                        .lengths()
                        .alias("common_activities_with_next_count"),
                    col("diff_activities_with_previous")
                        .list()
                        .lengths()
                        .alias("diff_activities_with_previous_count"),
                    col("diff_activities_with_next")
                        .list()
                        .lengths()
                        .alias("diff_activities_with_next_count"),
                    when(
                        col("activities_count")
                            .eq(lit(0))
                            .or(col("activities_count").is_null()),
                    )
                    .then(lit(0.0))
                    .otherwise(
                        col("common_activities_with_previous").list().lengths()
                            * (lit(1.0).div(col("activities_count"))),
                    )
                    .alias("common_activities_with_previous_factor"),
                    when(
                        col("activities_count")
                            .eq(lit(0))
                            .or(col("activities_count").is_null()),
                    )
                    .then(lit(0.0))
                    .otherwise(
                        col("common_activities_with_next").list().lengths()
                            * (lit(1.0).div(col("activities_count"))),
                    )
                    .alias("common_activities_with_next_factor"),
                    when(
                        col("activities_count")
                            .eq(lit(0))
                            .or(col("activities_count").is_null()),
                    )
                    .then(lit(0.0))
                    .otherwise(
                        col("diff_activities_with_previous").list().lengths()
                            * (lit(1.0).div(col("activities_count"))),
                    )
                    .alias("diff_activities_with_previous_factor"),
                    when(
                        col("activities_count")
                            .eq(lit(0))
                            .or(col("activities_count").is_null()),
                    )
                    .then(lit(0.0))
                    .otherwise(
                        col("diff_activities_with_next").list().lengths()
                            * (lit(1.0).div(col("activities_count"))),
                    )
                    .alias("diff_activities_with_next_factor"),
                ])
                .collect::<Vec<_>>()
        })
        .with_columns([
            when(col("is_sleep_entry"))
                .then(lit(0))
                .otherwise(
                    when(
                        col("mood_level_previous")
                            .lt_eq(15)
                            .and(col("mood_level_previous").lt(col("mood_level"))),
                    )
                    .then(col("mood_level_previous").div(lit(2)))
                    .otherwise(lit(0)),
                )
                .alias("mood_level_improvement_boost"),
            // moodlevel improbvement
            col("mood_level_improvement_previous_raw")
                .sign()
                .cast(DataType::Int8)
                .alias("mood_level_improvement_previous"),
            col("mood_level_improvement_raw")
                .sign()
                .cast(DataType::Int8)
                .alias("mood_level_improvement"),
            col("mood_level_improvement_next_raw")
                .sign()
                .cast(DataType::Int8)
                .alias("mood_level_improvement_next"),
        ])
        .with_columns([
            (col("mood_level_improvement_raw") + col("mood_level_improvement_boost"))
                .alias("mood_level_improvement_raw_with_boost"),
        ])
        .with_columns([
            col("mood_level_improvement_raw_with_boost")
                .sign()
                .cast(DataType::Int8)
                .alias("mood_level_improvement_with_boost"),
            when(col("is_sleep_entry"))
                .then(lit(0))
                .otherwise(
                    ((lit(-1)
                        * col("mood_level_improvement_previous_raw")
                        * col("common_activities_with_previous_factor"))
                        + (col("mood_level_improvement_raw_with_boost") * lit(5))
                        + (col("mood_level_improvement_next_raw")
                            * col("common_activities_with_next_factor")
                            * lit(2)))
                    .div(lit(8)),
                )
                .alias("mood_level_improvement_moving_average_raw"),
        ])
        .with_columns([when(col("is_sleep_entry"))
            .then(lit(0))
            .otherwise(
                ((lit(-1)
                    * col("mood_level_improvement_previous")
                    * col("common_activities_with_previous_factor"))
                    + (col("mood_level_improvement_with_boost") * lit(5))
                    + (col("mood_level_improvement_next")
                        * col("common_activities_with_next_factor")
                        * lit(2)))
                .div(lit(8)),
            )
            .alias("mood_level_improvement_moving_average")])
        .sort(
            "full_datetime",
            SortOptions {
                descending: false,
                multithreaded: true,
                ..Default::default()
            },
        )
        .with_columns([when(
            col("logical_date_stage_1")
                .neq_missing(col("full_date"))
                .and(col("time_numeric").gt_eq(lit(5.0))),
        )
        .then(col("full_date"))
        .otherwise(NULL.lit())
        .alias("logical_date_stage_2")])
        .with_columns([
            // logical date
            col("logical_date_stage_1")
                .fill_null(col("logical_date_stage_2"))
                .forward_fill(None)
                .backward_fill(None)
                .alias("logical_date"),
        ])
        .with_row_count("id", Some(1))
        .with_columns([when(col("logical_date").neq(col("full_date")))
            .then((col("id") + lit(LAST_MOMENT_OF_THE_DAY)).cast(DataType::Time))
            .otherwise(col("time"))
            .alias("logical_time")])
        .with_columns([(col("logical_date").cast(DataType::Utf8)
            + lit(" ")
            + col("logical_time").cast(DataType::Utf8))
        .str()
        .to_datetime(
            Some(TimeUnit::Milliseconds),
            None,
            StrptimeOptions {
                format: Some("%F %T%.f".to_string()),
                cache: false,
                exact: false,
                strict: false,
                ..Default::default()
            },
        )
        .alias("logical_full_datetime")]);

    let activities = lf_for_activities
        .clone()
        .select([col("activities")])
        .groupby([lit("")])
        .agg([col("activities")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .flatten()
            .unique()
            .drop_nulls()])
        .select([col("activities").explode()])
        .collect()?;

    let activities2 = activities.clone();
    let activities2 = activities2
        .column("activities")?
        .utf8()?
        .par_iter_indexed()
        .flatten()
        .map(String::from);
    let activities2 = activities2.collect::<Vec<_>>();

    let factor_col_names = FACTORS.values().flat_map(|factor| {
        factor.types().iter().map(move |&factor_type| {
            (
                factor_column_name(factor, factor_type),
                matches!(factor, Factor::MultipleValue { .. }),
                matches!(factor_type, FactorType::Taxonomy { .. }),
            )
        })
    });

    let factors = lf_for_factors
        .clone()
        .select(
            factor_col_names
                .clone()
                .map(|(col_name, is_multi_value, is_categorical)| {
                    if is_categorical {
                        if is_multi_value {
                            col(&col_name).cast(DataType::List(Box::new(DataType::Utf8)))
                        } else {
                            col(&col_name).cast(DataType::Utf8)
                        }
                    } else {
                        col(&col_name)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .groupby([lit("").alias("group")])
        .agg(
            factor_col_names
                .clone()
                .filter_map(|(col_name, _, is_categorical)| {
                    if is_categorical {
                        Some(col(&col_name).flatten().unique().drop_nulls())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        )
        .select([col("*").exclude(["group"])])
        .collect()?;

    let factors2 = factors
        .iter()
        .flat_map(|s| {
            s.list().unwrap().into_iter().map(|x| {
                x.map(|x| {
                    (
                        s.name().to_string(),
                        x.utf8()
                            .unwrap()
                            .par_iter()
                            .filter_map(|x| Some(x?.to_string()))
                            .collect::<Vec<_>>(),
                    )
                })
            })
        })
        .flatten()
        .collect::<HashMap<_, _>>();

    let fixed_col_order = [
        "id",
        "full_date",
        "time",
        "full_datetime",
        "logical_date",
        "logical_time",
        "logical_full_datetime",
        "day_of_year",
        "time_numeric",
    ]
    .into_iter()
    .chain(TIME_OF_DAY_INTERVALS.into_iter().map(|(&name, _)| name))
    .chain([
        "time_of_day",
        "is_sleep_entry",
        "note",
        "mood",
        "mood_level",
        "mood_level_previous",
        "mood_level_improvement_boost",
        "mood_level_improvement_previous_raw",
        "mood_level_improvement_raw",
        "mood_level_improvement_raw_with_boost",
        "mood_level_improvement_next_raw",
        "mood_level_improvement_previous",
        "mood_level_improvement",
        "mood_level_improvement_with_boost",
        "mood_level_improvement_next",
        "mood_level_improvement_moving_average_raw",
        "mood_level_improvement_moving_average",
    ])
    .map(String::from)
    .chain(FACTORS.into_iter().flat_map(|(_, factor)| {
        factor
            .types()
            .iter()
            .map(move |&factor_type| factor_column_name(factor, factor_type))
    }))
    .chain(
        factors2
            .iter()
            .filter(|(col_name, _)| {
                let (_, is_multiple, _) = factor_col_names
                    .clone()
                    .find(|(cname, _, _)| cname == *col_name)
                    .unwrap();

                is_multiple
            })
            .chain(factors2.iter().filter(|(col_name, _)| {
                let (_, is_multiple, _) = factor_col_names
                    .clone()
                    .find(|(cname, _, _)| cname == *col_name)
                    .unwrap();

                !is_multiple
            }))
            .flat_map(|(col_name, vals)| {
                // bind as borrow so it's not moved into the closure below
                //let factor_col_names = &factor_col_names;

                vals.clone()
                    .into_iter()
                    .map(move |factor| format!("{col_name}: {factor}"))
            }),
    )
    .chain(
        [
            "activities",
            "activities_previous",
            "activities_count",
            "activities_next",
            "common_activities_with_previous",
            "common_activities_with_previous_count",
            "common_activities_with_previous_factor",
            "common_activities_with_next",
            "common_activities_with_next_count",
            "common_activities_with_next_factor",
            "diff_activities_with_previous",
            "diff_activities_with_previous_count",
            "diff_activities_with_previous_factor",
            "diff_activities_with_next",
            "diff_activities_with_next_count",
            "diff_activities_with_next_factor",
        ]
        .map(String::from),
    )
    .chain(activities2.clone().into_iter().map(String::from));

    let lf1 = lf1
        .with_columns(
            activities2
                .iter()
                .map(|activity| {
                    col("activities")
                        .list()
                        .contains(lit(activity.clone()))
                        .alias(activity)
                })
                .chain(factors2.iter().flat_map(|(col_name, vals)| {
                    // bind as borrow so it's not moved into the closure below
                    //let factor_col_names = &factor_col_names;
                    let (_, is_multiple, _) = factor_col_names
                        .clone()
                        .find(|(cname, _, _)| cname == col_name)
                        .unwrap();
                    vals.iter().map(move |factor| {
                        if is_multiple {
                            col(col_name)
                                .list()
                                .contains(lit(factor.to_owned()))
                                .alias(&format!("{col_name}: {factor}"))
                        } else {
                            when(col(col_name).is_null())
                                .then(lit(false))
                                .otherwise(col(col_name).eq(lit(factor.to_owned())))
                                .alias(&format!("{col_name}: {factor}"))
                        }
                    })
                }))
                .collect::<Vec<_>>(),
        )
        .select(
            fixed_col_order
                .clone()
                .map(|col_name| col(&col_name))
                .chain([col("*").exclude(fixed_col_order.chain([
                    "matched_factors".to_string(),
                    "logical_date_stage_1".to_string(),
                    "logical_date_stage_2".to_string(),
                    "weekday".to_string(),
                    "date".to_string(),
                    "note_title".to_string(),
                ]))])
                .collect::<Vec<_>>(),
        );

    Ok(ProcessedData {
        dataframe: lf1,
        factors: factors2,
        activities: activities2,
    })
}
