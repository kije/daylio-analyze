use daylio_analyze::{process, ProcessError};
use polars::prelude::*;
use polars::prelude::{CsvWriter, LazyCsvReader, LazyFileListReader};
use std::path::Path;

pub fn main() -> Result<(), ProcessError> {
    let lf1 = LazyCsvReader::new("daylio_export.csv")
        .with_cache(true)
        .with_infer_schema_length(Some(15))
        .with_try_parse_dates(false)
        .with_cache(true)
        .finish()?;

    let lf1 = process(lf1)?;

    let lfp = lf1.dataframe.clone().profile()?;

    println!("{:#?}", lfp);
    //
    // let mut export_lf1 = lf1
    //     .with_columns(
    //         factor_col_names
    //             .clone()
    //             .flat_map(|(col_name, is_multi_value, _)| {
    //                 if !is_multi_value {
    //                     return None;
    //                 }
    //
    //                 Some(
    //                     col(&col_name)
    //                         .cast(DataType::List(Box::new(DataType::Utf8)))
    //                         .list()
    //                         .join(" | "),
    //                 )
    //             })
    //             .chain([
    //                 col("activities")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //                 col("activities_previous")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //                 col("activities_next")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //                 col("common_activities_with_previous")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //                 col("common_activities_with_next")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //                 col("diff_activities_with_previous")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //                 col("diff_activities_with_next")
    //                     .cast(DataType::List(Box::new(DataType::Utf8)))
    //                     .list()
    //                     .join(" | "),
    //             ])
    //             .collect::<Vec<_>>(),
    //     )
    //     .collect()?;
    //
    // let mut file = std::fs::File::create("daylio_export_processed.csv")?;
    // CsvWriter::new(&mut file).finish(&mut export_lf1)?;
    Ok(())
}
