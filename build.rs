use proc_macro2::Span;
use quote::quote;
use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::string::ToString;

use const_gen::*;
use syn::parse_quote;

#[derive(CompileConst)]
#[allow(dead_code)]
enum FactorType {
    Scale {
        name: Option<&'static str>,
        tag: &'static str,
    },
    Taxonomy {
        name: Option<&'static str>,
        tag: &'static str,
    },
}

impl CompileConst for &'static FactorType {
    fn const_type() -> String {
        "&'static FactorType".to_string()
    }

    fn const_val(&self) -> String {
        format!("&{}", self.to_owned().const_val())
    }
}

#[derive(CompileConst)]
#[allow(dead_code)]
enum Factor {
    SingleValue {
        name: &'static str,
        types: &'static [&'static FactorType],
        tag: &'static str,
    },
    MultipleValue {
        name: &'static str,
        types: &'static [&'static FactorType],
        tag: &'static str,
    },
}

#[derive(CompileConst)]
#[allow(dead_code)]
struct MoodData {
    pub name: String,
    pub level: u8,
    pub label: String,
    pub label_short: String,
}

#[derive(Clone, Debug)]
struct MoodDataRaw(
    Cow<'static, str>,
    u8,
    Cow<'static, str>,
    Cow<'static, str>,
    Cow<'static, str>,
);

impl From<MoodDataRaw> for MoodData {
    fn from(
        MoodDataRaw(name, numeric_mood, mood_label, mood_label_short, ..): MoodDataRaw,
    ) -> Self {
        Self {
            name: name.into(),
            level: numeric_mood,
            label: mood_label.into(),
            label_short: mood_label_short.into(),
        }
    }
}

static BASE_MOODS: [MoodDataRaw; 6] = [
    MoodDataRaw(
        Cow::Borrowed("Really good"),
        60,
        Cow::Borrowed("ReallyGood"),
        Cow::Borrowed("sehr gute Stimmung"),
        Cow::Borrowed("sehr gut"),
    ),
    MoodDataRaw(
        Cow::Borrowed("Good"),
        50,
        Cow::Borrowed("Good"),
        Cow::Borrowed("gute Stimmung"),
        Cow::Borrowed("gut"),
    ),
    MoodDataRaw(
        Cow::Borrowed("Medium"),
        40,
        Cow::Borrowed("Medium"),
        Cow::Borrowed("mittelmässige Stimmung"),
        Cow::Borrowed("mittelmässig/ok"),
    ),
    MoodDataRaw(
        Cow::Borrowed("Not so good"),
        30,
        Cow::Borrowed("NotSoGood"),
        Cow::Borrowed("weniger gute Stimmung"),
        Cow::Borrowed("weniger gut"),
    ),
    MoodDataRaw(
        Cow::Borrowed("Bad"),
        20,
        Cow::Borrowed("Bad"),
        Cow::Borrowed("schlechte Stimmung"),
        Cow::Borrowed("schlecht"),
    ),
    MoodDataRaw(
        Cow::Borrowed("Awful"),
        10,
        Cow::Borrowed("Awful"),
        Cow::Borrowed("sehr schlechte Stimmung/unerträglich"),
        Cow::Borrowed("sehr schlecht/unerträglich"),
    ),
];

static ACTIVITIES_MAP: [(&str, &str); 41] = [
    ("[143] journaling", "[143] Tagebuch schreiben"),
    (
        "[156] spend time w/parents",
        "[156] Mit seinen Eltern zusammen sein",
    ),
    (
        "[189] mindful meditation",
        "[189] Meditation oder Yoga betreiben/Achtsamkeit",
    ),
    ("[209] listening to music", "[209] Musik hören"),
    ("[253] take a walk", "[253] Einen Spaziergang machen"),
    ("[46] shower", "[46] Eine Dusche nehmen"),
    (
        "[55] think abt. self/probl",
        "[55] Über sich selbst oder seine Probleme nachdenken",
    ),
    ("[69] taking a nap", "[69] Ein Nickerchen machen"),
    (
        "[264] think interesting st",
        "[264] Über eine interessante Frage nachdenken",
    ),
    (
        "[43] Eating out w/ smbd",
        "[43] Mit Freunden oder Bekannten zusammen essen",
    ),
    ("[118] being outside", "[118] Sich im Freien aufhalten"),
    (
        "[70] spend time w/ friends",
        "[70] Mit Freunden zusammen sein",
    ),
    (
        "[94] sitting in the sun",
        "[94] In der Sonne sitzen/aufhalten",
    ),
    (
        "[226] cooking a new meal",
        "[226] Ein neues oder spezielles Gericht zubereiten",
    ),
    ("[12] crafts/be creative", "[12] Kreatives arbeiten/basteln"),
    ("[228] shopping", "[228] Shoppig/Einkaufen"),
    ("[128] helping someone", "[128] Jemandem helfen"),
    ("[135] eat well", "[135] Gut essen/Essen gehen"),
    (
        "[191] talk to work colleag",
        "[191] Mit Arbeitskollegen (oder Klassenkamerden) sprechen",
    ),
    ("[158] phone call", "[158] Telefongespräche führen"),
    ("[167] cooking", "[167] Essen kochen"),
    ("[1] be in nature", "[1] Ins Grüne fahren/in der Natur sein"),
    (
        "[52] explore new surround.",
        "[52] Erkundungen machen/unbekannte Gegenden erforschen",
    ),
    // other activities
    ("Groceries", "Einkauf erledigen"),
    ("conversation", "Konversation"),
    ("eating", "Eine Mahlzeit einnehmen"),
    ("lay in bed", "Im Bett liegen (wach)"),
    ("make/drink coffee", "Kaffee machen/trinken"),
    ("meeting", "An einem Meeting teilnehmen"),
    ("selbsthilfe gruppe", "Selbsthilfegruppe"),
    ("sleep (night)", "Schlafen (in der Nacht)"),
    ("watch video", "Videos/Medien schauen/konsumieren"),
    ("working", "Arbeiten"),
    ("working (private)", "Privates Arbeiten"),
    ("social media", "Auf Social Media"),
    ("washing clothes", "Wäsche machen"),
    ("chat/write w/ smbdy", "Mit jemandem chatten (online)"),
    ("taking SSRI", "Antidepresiva einnehmen"),
    ("therapy", "Therapie/Psychiater"),
    ("cleaning", "Wohnung putzen"),
    ("visit bakery", "Zur Bäckerei gehen"),
];

#[derive(Clone, Debug)]
struct TimeOfDayInterval(&'static str, f32, f32, bool);

static TIME_OF_DAY_INTERVALS: [TimeOfDayInterval; 4] = [
    TimeOfDayInterval("IsMorning", 6.0, 13.0, false),
    TimeOfDayInterval("IsAfternoon", 13.0, 17.0, false),
    TimeOfDayInterval("IsEvening", 17.0, 23.0, false),
    TimeOfDayInterval("IsNight", 23.0, 6.0, true),
];

#[derive(CompileConst)]
#[allow(dead_code)]
#[inherit_doc]
enum TimeBlock {
    SameDay {
        name: &'static str,
        from: f32,
        to: f32,
    },
    #[inherit_doc]
    /// The time block is across two days, e.g. 23:00 - 01:00
    /// the selected time range is calculated as `time` < `before` on the next day & `time` >= `after` on the same day
    AcrossDays {
        name: &'static str,
        before: f32,
        after: f32,
    },
}

impl From<TimeOfDayInterval> for TimeBlock {
    fn from(value: TimeOfDayInterval) -> Self {
        let TimeOfDayInterval(name, from, to, is_next_day) = value;
        if is_next_day {
            TimeBlock::AcrossDays {
                name,
                before: to,
                after: from,
            }
        } else {
            TimeBlock::SameDay { name, from, to }
        }
    }
}

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("consts-codegen.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    const MOOD_ENUM_NAME: &str = "Mood";
    const DIMINISHED_ENUM_VARIANT_POSTFIX: &str = "Minus";

    let mut base_mood_2_enum = phf_codegen::OrderedMap::new();

    let activities_map = ACTIVITIES_MAP.into_iter().collect::<HashMap<&str, &str>>();

    for m @ MoodDataRaw(mood, numeric_mood, enum_variant, mood_label, mood_label_short) in
        &BASE_MOODS
    {
        base_mood_2_enum.entry(
            mood.to_string(),
            &format!(
                "{MOOD_ENUM_NAME}::{enum_variant}({})",
                MoodData::from(m.clone()).const_val()
            ),
        );

        let numeric_mood_new = numeric_mood - 5;
        let m2 = MoodDataRaw(
            format!("{mood} [-]").into(),
            numeric_mood_new,
            format!("{enum_variant}{DIMINISHED_ENUM_VARIANT_POSTFIX}").into(),
            format!("{mood_label} (-)").into(),
            format!("{mood_label_short} (-)").into(),
        );
        base_mood_2_enum.entry(
            format!("{mood} [-]"),
            &format!(
                "{MOOD_ENUM_NAME}::{enum_variant}{DIMINISHED_ENUM_VARIANT_POSTFIX}({})",
                MoodData::from(m2).const_val()
            ),
        );
    }

    let time_blocks = TIME_OF_DAY_INTERVALS
        .iter()
        .map(|time_of_day_interval| {
            (
                time_of_day_interval.0,
                TimeBlock::from(time_of_day_interval.clone()),
            )
        })
        .collect::<HashMap<_, _>>();

    writeln!(
        &mut file,
        "{}",
        const_definition!(#[derive(PartialEq, PartialOrd, Debug, Clone, Copy)] #[non_exhaustive] pub(crate) TimeBlock)
    )
    .unwrap();

    writeln!(
        &mut file,
        "{}",
        static_declaration!(pub(crate) TIME_OF_DAY_INTERVALS = time_blocks)
    )
    .unwrap();

    writeln!(
        &mut file, "{}", 
        const_definition!(#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, ::enum_access::EnumAccess)] #[non_exhaustive] #[enum_access(get(tag = "pub", name = "pub"))]  pub FactorType)
    ).unwrap();

    writeln!(
        &mut file,
        "{}",
        const_definition!(#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, ::enum_access::EnumAccess)] #[enum_access(get(name = "pub", tag = "pub", types = "pub"))]  pub  Factor)
    )
    .unwrap();

    static FACTOR_TYPE_F: FactorType = FactorType::Scale {
        name: None,
        tag: "f",
    };
    static FACTOR_TYPE_Q: FactorType = FactorType::Scale {
        name: Some("Qualität"),
        tag: "q",
    };
    static FACTOR_TYPE_C: FactorType = FactorType::Taxonomy {
        name: None,
        tag: "c",
    };

    writeln!(
        &mut file,
        "{}\n{}\n{}",
        static_declaration!(pub(crate) FACTOR_TYPE_F = FACTOR_TYPE_F),
        static_declaration!(pub(crate) FACTOR_TYPE_Q = FACTOR_TYPE_Q),
        static_declaration!(pub(crate) FACTOR_TYPE_C = FACTOR_TYPE_C),
    )
    .unwrap();

    writeln!(
        &mut file,
        "pub static FACTOR_TYPES: ::phf::Map<&'static str, &'static FactorType> = {};",
        phf_codegen::Map::new()
            .entry("f", "&FACTOR_TYPE_F")
            .entry("q", "&FACTOR_TYPE_Q")
            .entry("c", "&FACTOR_TYPE_C")
            .build()
    )
    .unwrap();

    writeln!(
        &mut file,
        "pub static FACTORS: ::phf::Map<&'static str, Factor> = {};",
        phf_codegen::Map::new()
            .entry(
                "sl",
                &quote!(Factor::SingleValue {
                    name: "Schlaf",
                    types: &[&FACTOR_TYPE_F, &FACTOR_TYPE_Q],
                    tag: "sl"
                })
                .to_string()
            )
            .entry(
                "f",
                &quote!(Factor::MultipleValue {
                    name: "Funktionsfähigkeit",
                    types: &[&FACTOR_TYPE_C],
                    tag: "f"
                })
                .to_string()
            )
            .entry(
                "st",
                &quote!(Factor::SingleValue {
                    name: "Stress",
                    types: &[&FACTOR_TYPE_F],
                    tag: "st"
                })
                .to_string()
            )
            .entry(
                "e",
                &quote!(Factor::MultipleValue {
                    name: "Emotionen",
                    types: &[&FACTOR_TYPE_C],
                    tag: "e"
                })
                .to_string()
            )
            .entry(
                "med",
                &quote!(Factor::MultipleValue {
                    name: "Medikation",
                    types: &[&FACTOR_TYPE_C],
                    tag: "med"
                })
                .to_string()
            )
            .entry(
                "w",
                &quote!(Factor::SingleValue {
                    name: "Wetter",
                    types: &[&FACTOR_TYPE_C],
                    tag: "w"
                })
                .to_string()
            )
            .entry(
                "sy",
                &quote!(Factor::MultipleValue {
                    name: "Symptome",
                    types: &[&FACTOR_TYPE_C],
                    tag: "sy"
                })
                .to_string()
            )
            .build()
    )
    .unwrap();

    writeln!(&mut file, "{}", const_definition!(#[derive(PartialEq, PartialOrd, Debug, Clone, Copy)] #[non_exhaustive] pub  MoodData)).unwrap();

    let mood_enum_variants_tokens = BASE_MOODS
        .iter()
        .map(|MoodDataRaw(_, _, enum_variant, ..)| {
            [
                enum_variant.to_string(),
                format!("{enum_variant}{DIMINISHED_ENUM_VARIANT_POSTFIX}"),
            ]
        });

    let mood_enum_ident = syn::Ident::new(MOOD_ENUM_NAME, Span::call_site());
    let mood_enum_kind_ident =
        syn::Ident::new(&format!("{}Kind", MOOD_ENUM_NAME), Span::call_site());

    let mood_enum_variants_tokens2 =
        mood_enum_variants_tokens
            .flatten()
            .map(|enum_variant| syn::Variant {
                attrs: vec![],
                ident: syn::Ident::new(&enum_variant, Span::call_site()),
                fields: syn::Fields::Unnamed(syn::FieldsUnnamed {
                    paren_token: syn::token::Paren(Span::call_site()),
                    unnamed: {
                        let mut unnamed = syn::punctuated::Punctuated::new();
                        unnamed.push(syn::Field {
                            attrs: vec![parse_quote! {#[enum_alias(mood_data)]}],
                            vis: syn::Visibility::Inherited,
                            mutability: syn::FieldMutability::None,
                            ident: None,
                            colon_token: None,
                            ty: parse_quote!(MoodData),
                        });
                        unnamed
                    },
                }),
                discriminant: None,
            });

    let mood_enum_tokens = quote! {
        #[derive(PartialEq, Debug, Clone, PartialOrd, Copy, ::enum_access::EnumAccess, ::enum_kinds::EnumKind)]
        #[enum_kind(#mood_enum_kind_ident)]
        #[non_exhaustive]
        #[enum_access(get(mood_data = "pub"))]
        pub enum #mood_enum_ident {
            #(#mood_enum_variants_tokens2),*
        }
    };

    writeln!(&mut file, "{}", mood_enum_tokens).unwrap();

    writeln!(
        &mut file,
        "pub static MOOD_2_MOOD_ENUM: ::phf::OrderedMap<&'static str, {}> = {};",
        MOOD_ENUM_NAME,
        base_mood_2_enum.build()
    )
    .unwrap();

    writeln!(
        &mut file,
        "{}",
        static_declaration!(pub(crate) ACTIVITIES_MAP = activities_map)
    )
    .unwrap();

    println!("cargo:rerun-if-changed=build.rs");
}
