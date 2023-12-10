use polars::prelude::*;

use daylio_analyze::{process, ProcessError};

pub fn main() -> Result<(), ProcessError> {
    let lf1 = LazyCsvReader::new("./daylio_export.csv")
        .with_cache(true)
        .with_infer_schema_length(Some(5))
        .with_try_parse_dates(false)
        .finish()?;

    let lf1 = process(lf1)?;

    let lfp = lf1.dataframe.clone().collect()?;

    println!("{:#?}", lfp);

    let columns = [col("activities")
        .cast(DataType::List(Box::new(DataType::Utf8)))
        .list()
        .join(lit(" | "))];

    let columns = columns.into_iter();


    #[cfg(feature = "process_activities")]
        let columns = columns.chain([
        col("activities_previous")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .list()
            .join(lit(" | ")),
        col("activities_next")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .list()
            .join(lit(" | ")),
        col("common_activities_with_previous")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .list()
            .join(lit(" | ")),
        col("common_activities_with_next")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .list()
            .join(lit(" | ")),
        col("diff_activities_with_previous")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .list()
            .join(lit(" | ")),
        col("diff_activities_with_next")
            .cast(DataType::List(Box::new(DataType::Utf8)))
            .list()
            .join(lit(" | ")),
    ].into_iter());

    #[cfg(feature = "process_factors")]
        let columns = columns.chain(lf1.factors
        .iter()
        .flat_map(|c| {
            if !c.is_multiple() {
                return None;
            }

            Some(
                col(&c.column_name)
                    .cast(DataType::List(Box::new(DataType::Utf8)))
                    .list()
                    .join(lit(" | ")),
            )
        }));


    let mut export_lf1 = lf1
        .dataframe
        .with_columns(
            columns.collect::<Vec<_>>(),
        )
        .collect()?;

    let mut file = std::fs::File::create("../daylio_export_processed.csv")?;
    CsvWriter::new(&mut file).finish(&mut export_lf1)?;
    Ok(())
}
