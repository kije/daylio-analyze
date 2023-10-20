use std::borrow::Cow;
#[cfg(any(feature = "process_factors", feature = "process_activities"))]
use std::collections::HashMap;
use std::ops::Div;

use polars::export::rayon::prelude::*;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use thiserror::Error;

pub use crate::consts::{
    Factor, FactorType, Mood, MoodData, ACTIVITIES_MAP, FACTORS, FACTOR_TYPES, MOOD_2_MOOD_ENUM,
    MAX_MOOD_LEVEL,
    MIN_MOOD_LEVEL,
};
use crate::consts::{
    TimeBlock,
    LAST_MOMENT_OF_THE_DAY, SLEEP_ACTIVITY, TIME_OF_DAY_INTERVALS,
};

#[cfg(feature = "process_factors")]
use crate::consts::{FACTOR_TAG_RE};

pub(crate) mod consts;
pub mod utils;

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error(transparent)]
    Polars(#[from] PolarsError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("schema mismatch")]
    SchemaMismatch,
    #[error("unknown processing error")]
    Unknown,
}

type ColumnName = Cow<'static, str>;

#[cfg(feature = "process_activities")]
type ActivityValue = Cow<'static, str>;

#[cfg(feature = "process_activities")]
#[derive(Clone, Debug)]
pub struct ActivityColumn {
    pub column_name: ColumnName,
    pub values: Vec<ActivityValue>,
    pub binary_columns: HashMap<ActivityValue, ColumnName>,
}

type ColumnExprIter = Box<dyn Iterator<Item=Expr>>;

#[cfg(all(not(feature = "no_proccess"), feature = "process_activities"))]
#[inline(always)]
fn process_activities(lf: LazyFrame) -> Result<(ActivityColumn, ColumnExprIter), ProcessError> {
    let activities = lf
        .select([col("activities")])
        .group_by([lit("")])
        .agg([col("activities")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .flatten()
            .unique()
            .drop_nulls()])
        .select([col("activities").explode()])
        .collect()?;

    let activities = activities
        .column("activities")?
        .utf8()?
        .par_iter_indexed()
        .flatten()
        .map(String::from);

    let activities = activities.map(ColumnName::from).collect::<Vec<_>>();

    let activities_binary_columns = activities.clone().into_iter().map(|activity| {
        col("activities")
            .alias(&activity)
            .list()
            .contains(activity.lit())
    });

    Ok((
        ActivityColumn {
            column_name: ColumnName::from("activities"),
            binary_columns: activities.iter().map(|a| (a.clone(), a.clone())).collect(),
            values: activities,
        },
        Box::new(activities_binary_columns),
    ))
}

#[cfg(feature = "process_factors")]
type FactorValue = Cow<'static, str>;

#[cfg(feature = "process_factors")]
#[derive(Clone, Debug)]
pub struct FactorColumn {
    pub factor: &'static Factor,
    pub factor_type: &'static FactorType,
    pub column_name: ColumnName,
    pub values: Option<Vec<FactorValue>>,
    pub binary_columns: Option<HashMap<FactorValue, ColumnName>>,
}

#[cfg(feature = "process_factors")]
impl FactorColumn {
    pub const fn is_multiple(&self) -> bool {
        matches!(self.factor, Factor::MultipleValue { .. })
    }

    pub const fn is_categorical(&self) -> bool {
        matches!(self.factor_type, FactorType::Taxonomy { .. })
    }
}

#[cfg(all(not(feature = "no_proccess"), feature = "process_factors"))]
#[inline(always)]
fn factor_column_name(factor: &Factor, factor_type: &FactorType) -> ColumnName {
    let tag = factor.tag();
    let factor_type_tag = factor_type.tag();

    let mut s = format!("factor_{factor_type_tag}_{tag}");
    s.truncate(s.trim_end().len());
    ColumnName::from(s)
}

#[cfg(all(not(feature = "no_proccess"), feature = "process_factors"))]
#[inline(always)]
fn process_factors(lf: LazyFrame) -> Result<(Vec<FactorColumn>, ColumnExprIter), ProcessError> {
    let factor_col_names = FACTORS.values().flat_map(|factor| {
        factor.types().iter().map(move |&factor_type| FactorColumn {
            factor,
            factor_type,
            column_name: factor_column_name(factor, factor_type),
            values: None,
            binary_columns: None,
        })
    });

    let factors_df = lf
        .select(
            factor_col_names
                .clone()
                .map(|c| {
                    if c.is_categorical() {
                        if c.is_multiple() {
                            col(&c.column_name).cast(DataType::List(Box::new(DataType::Utf8)))
                        } else {
                            col(&c.column_name).cast(DataType::Utf8)
                        }
                    } else {
                        col(&c.column_name)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .group_by([lit("").alias("group")])
        .agg(
            factor_col_names
                .clone()
                .filter_map(|c| {
                    if c.is_categorical() {
                        Some(col(&c.column_name).flatten().unique().drop_nulls())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        )
        .select([col("*").exclude(["group"])])
        .collect()?;

    let factors = factors_df
        .iter()
        .flat_map(|s| {
            s.list().unwrap().into_iter().map(|x| {
                x.map(|x| {
                    (
                        ColumnName::from(s.name().to_string()),
                        x.utf8()
                            .unwrap()
                            .par_iter()
                            .filter_map(|x| x.map(|x| FactorValue::from(x.to_string())))
                            .collect::<Vec<_>>(),
                    )
                })
            })
        })
        .flatten()
        .collect::<HashMap<_, Vec<_>>>();

    let factors_columns = factor_col_names
        .map(|mut c| {
            c.values = factors.get(c.column_name.as_ref()).and_then(|x| {
                if x.is_empty() {
                    None
                } else {
                    Some(x.to_owned())
                }
            });
            c.binary_columns = c.values.clone().map(|values| {
                values
                    .par_iter()
                    .map(|v| {
                        (
                            v.clone(),
                            ColumnName::from(format!("{}: {}", c.column_name, v)),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            });

            c
        })
        .collect::<Vec<_>>();

    let factors_columns_iterator = factors_columns
        .clone()
        .into_iter()
        .flat_map(|factor_col| {
            let column_name = factor_col.column_name.clone();
            let is_categorical = (factor_col).is_categorical();
            let is_multiple = (factor_col).is_multiple();
            factor_col.binary_columns.map(move |binary_columns| {
                binary_columns.into_iter().map(move |(v, c)| {
                    if is_categorical {
                        if is_multiple {
                            col(&column_name).list().contains(v.lit())
                        } else {
                            when(col(&column_name).is_null())
                                .then(lit(false))
                                .otherwise(col(&column_name).eq(v.lit()))
                        }
                    } else {
                        col(&column_name)
                    }
                        .alias(&c)
                })
            })
        })
        .flatten();

    Ok((factors_columns, Box::new(factors_columns_iterator)))
}

#[inline(always)]
pub fn check_schema(lf: LazyFrame) -> Result<LazyFrame, ProcessError> {
    let schema = lf.schema()?;

    if !schema.contains("full_date")
        || !schema.contains("time")
        || !schema.contains("activities")
        || !schema.contains("note")
        || schema.get("full_date").unwrap() != &DataType::Utf8
        || schema.get("time").unwrap() != &DataType::Utf8
        || schema.get("activities").unwrap() != &DataType::Utf8
    {
        return Err(ProcessError::SchemaMismatch);
    }

    Ok(lf)
}

#[cfg(not(feature = "no_proccess"))]
#[derive(Clone)]
pub struct ProcessedData {
    pub dataframe: LazyFrame,
    #[cfg(feature = "process_factors")]
    pub factors: Vec<FactorColumn>,
    #[cfg(feature = "process_activities")]
    pub activities: ActivityColumn,
}

#[cfg(not(feature = "no_proccess"))]
pub fn process(lf: LazyFrame) -> Result<ProcessedData, ProcessError> {
    let lf = check_schema(lf)?;

    #[cfg(feature = "cse")]
        let lf = lf.with_comm_subexpr_elim(true);

    let lf = lf
        .with_columns([
            // activities
            col("activities")
                .str()
                .split("|".lit())
                .list()
                .eval(col("").str().strip_chars(None).filter(col("").neq(lit(""))), true)
                .list()
                .unique(),
        ]);

    #[cfg(feature = "process_factors")]
        let lf = lf
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
        ]);

    let lf = lf.cache();

    #[cfg(feature = "process_activities")]
        let lf_for_activities = lf.clone();

    #[cfg(feature = "process_factors")]
        let lf = lf
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

    #[cfg(feature = "process_factors")]
        let lf_for_factors = lf.clone();

    let lf = lf
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
            #[cfg(feature = "process_activities")]
                { col("activities").list().lengths().alias("activities_count") },
            #[cfg(feature = "process_activities")]
                { col("activities").shift(-1).alias("activities_previous") },
            #[cfg(feature = "process_activities")]
                { col("activities").shift(1).alias("activities_next") },
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
                    lit("raise"),
                )
                .alias("full_datetime"),
            // FIXME
            // col("full_date")
            //     .dt()
            //     .strftime("%j")
            //     .cast(DataType::UInt16)
            //     .alias("day_of_year"),
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
            #[cfg(feature = "process_activities")]
                { col("activities_previous").cast(DataType::List(Box::new(DataType::Categorical(None)))) },
            #[cfg(feature = "process_activities")]
                { col("activities_next").cast(DataType::List(Box::new(DataType::Categorical(None)))) },
            #[cfg(feature = "process_activities")]
                {
                    col("activities")
                        .list()
                        .set_intersection(col("activities_previous"))
                        .list()
                        .unique()
                        .cast(DataType::List(Box::new(DataType::Categorical(None))))
                        .alias("common_activities_with_previous")
                },
            #[cfg(feature = "process_activities")]
                {
                    col("activities")
                        .list()
                        .set_intersection(col("activities_next"))
                        .list()
                        .unique()
                        .cast(DataType::List(Box::new(DataType::Categorical(None))))
                        .alias("common_activities_with_next")
                },
            #[cfg(feature = "process_activities")]
                {
                    col("activities")
                        .list()
                        .set_difference(col("activities_previous"))
                        .list()
                        .unique()
                        .cast(DataType::List(Box::new(DataType::Categorical(None))))
                        .alias("diff_activities_with_previous")
                },
            #[cfg(feature = "process_activities")]
                {
                    col("activities")
                        .list()
                        .set_difference(col("activities_next"))
                        .list()
                        .unique()
                        .cast(DataType::List(Box::new(DataType::Categorical(None))))
                        .alias("diff_activities_with_next")
                },
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
                    #[cfg(feature = "process_activities")]
                        {
                            col("common_activities_with_previous")
                                .list()
                                .lengths()
                                .alias("common_activities_with_previous_count")
                        },
                    #[cfg(feature = "process_activities")]
                        {
                            col("common_activities_with_next")
                                .list()
                                .lengths()
                                .alias("common_activities_with_next_count")
                        },
                    #[cfg(feature = "process_activities")]
                        {
                            col("diff_activities_with_previous")
                                .list()
                                .lengths()
                                .alias("diff_activities_with_previous_count")
                        },
                    #[cfg(feature = "process_activities")]
                        {
                            col("diff_activities_with_next")
                                .list()
                                .lengths()
                                .alias("diff_activities_with_next_count")
                        },
                    #[cfg(feature = "process_activities")]
                        {
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
                                .alias("common_activities_with_previous_factor")
                        },
                    #[cfg(feature = "process_activities")]
                        {
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
                                .alias("common_activities_with_next_factor")
                        },
                    #[cfg(feature = "process_activities")]
                        {
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
                                .alias("diff_activities_with_previous_factor")
                        },
                    #[cfg(feature = "process_activities")]
                        {
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
                                .alias("diff_activities_with_next_factor")
                        },
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
            #[cfg(feature = "process_activities")]
                {
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
                        .alias("mood_level_improvement_moving_average_raw")
                },
        ]);

    #[cfg(feature = "process_activities")]
        let lf = lf
        .with_columns([
            when(col("is_sleep_entry"))
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
                .alias("mood_level_improvement_moving_average")
        ]);

    let lf = lf
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
                lit("raise"),
            )
            .alias("logical_full_datetime")])
        ;

    #[cfg(feature = "process_activities")]
        let (activities, activities_columns) = process_activities(lf_for_activities)?;

    #[cfg(feature = "process_factors")]
        let (factors, factor_columns) = process_factors(lf_for_factors)?;

    let fixed_col_order = [
        "id",
        "full_date",
        "time",
        "full_datetime",
        "logical_date",
        "logical_time",
        "logical_full_datetime",
        //"day_of_year", // FIXME
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
            #[cfg(feature = "process_activities")]
                "mood_level_improvement_moving_average_raw",
            #[cfg(feature = "process_activities")]
                "mood_level_improvement_moving_average",
        ])
        .map(ColumnName::from);

    #[cfg(feature = "process_factors")]
        let fixed_col_order = fixed_col_order
        .chain(
            factors
                .iter()
                .filter(|c| c.is_multiple())
                .chain(factors.iter().filter(|c| !c.is_multiple()))
                .flat_map(|c| {
                    [c.column_name.clone()].into_iter().chain(
                        c.binary_columns
                            .clone()
                            .unwrap_or_default()
                            .values()
                            .cloned()
                            .collect::<Vec<_>>(),
                    )
                }),
        );

    let fixed_col_order = fixed_col_order
        .chain(
            [
                "activities",
                #[cfg(feature = "process_activities")]
                    "activities_previous",
                #[cfg(feature = "process_activities")]
                    "activities_count",
                #[cfg(feature = "process_activities")]
                    "activities_next",
                #[cfg(feature = "process_activities")]
                    "common_activities_with_previous",
                #[cfg(feature = "process_activities")]
                    "common_activities_with_previous_count",
                #[cfg(feature = "process_activities")]
                    "common_activities_with_previous_factor",
                #[cfg(feature = "process_activities")]
                    "common_activities_with_next",
                #[cfg(feature = "process_activities")]
                    "common_activities_with_next_count",
                #[cfg(feature = "process_activities")]
                    "common_activities_with_next_factor",
                #[cfg(feature = "process_activities")]
                    "diff_activities_with_previous",
                #[cfg(feature = "process_activities")]
                    "diff_activities_with_previous_count",
                #[cfg(feature = "process_activities")]
                    "diff_activities_with_previous_factor",
                #[cfg(feature = "process_activities")]
                    "diff_activities_with_next",
                #[cfg(feature = "process_activities")]
                    "diff_activities_with_next_count",
                #[cfg(feature = "process_activities")]
                    "diff_activities_with_next_factor",
            ]
                .map(ColumnName::from),
        );

    #[cfg(feature = "process_activities")]
        let fixed_col_order = fixed_col_order
        .chain(activities.binary_columns.values().cloned());

    #[cfg(all(feature = "process_activities", feature = "process_factors"))]
        let lf = lf
        .with_columns(activities_columns.chain(factor_columns).collect::<Vec<_>>());

    #[cfg(all(feature = "process_activities", not(feature = "process_factors")))]
        let lf = lf
        .with_columns(activities_columns.collect::<Vec<_>>());

    #[cfg(all(not(feature = "process_activities"), feature = "process_factors"))]
        let lf = lf
        .with_columns(factor_columns.collect::<Vec<_>>());

    let lf = lf
        .select(
            fixed_col_order
                .clone()
                .map(|col_name| col(&col_name))
                .chain([
                    col("*").exclude(
                        fixed_col_order.chain(
                            [
                                #[cfg(feature = "process_factors")]
                                    "matched_factors",
                                "logical_date_stage_1",
                                "logical_date_stage_2",
                                "weekday",
                                "date",
                                "note_title",
                            ]
                                .map(ColumnName::from),
                        ),
                    )])
                .collect::<Vec<_>>(),
        )
        ;

    Ok(ProcessedData {
        dataframe: lf,
        #[cfg(feature = "process_factors")]
        factors,
        #[cfg(feature = "process_activities")]
        activities,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_process_simple_dataframe() {
        let df = DataFrame::new(vec![
            Series::new(
                "full_date",
                &[
                    "2021-01-04",
                    "2021-01-04",
                    "2021-01-03",
                    "2021-01-03",
                    "2021-01-02",
                    "2021-01-02",
                    "2021-01-01",
                ],
            ),
            Series::new(
                "time",
                &[
                    "00:00", "12:00", "18:00", "19:00", "03:00", "12:00", "19:00",
                ],
            ),
            Series::new(
                "activities",
                &[
                    "foo",
                    "",
                    "bar | baz",
                    "foo | baz",
                    "",
                    SLEEP_ACTIVITY,
                    "foo | <c_e> sad | <f_sl100> good",
                ],
            ),
            Series::new("mood", &["1", "1", "2", "3", "2", "6", "5"]),
            Series::new("note", &["", "", "", "", "", "", ""]),
        ])
            .unwrap();

        let result = process(df.lazy()).unwrap();

        assert_eq!(
            result.factors.len(),
            FACTORS.values().flat_map(|f| f.types().iter()).count()
        );

        let expected_activities = ["foo", "bar", "baz", SLEEP_ACTIVITY].map(ColumnName::from);

        assert!(expected_activities
            .iter()
            .all(|item| result.activities.values.contains(item)));
        assert!(result
            .activities
            .values
            .iter()
            .all(|item| expected_activities.contains(item)));

        let df_processed = result.dataframe.collect().unwrap();

        assert_eq!(df_processed.shape(), (7, 59));

        println!("{:#?}", df_processed);

        // todo test more
    }
}
