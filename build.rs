use nom::{
    IResult, Parser,
    branch::alt,
    bytes::{complete::take_until, is_not, tag},
    character::multispace0,
    multi::many0,
    sequence::{delimited, preceded, separated_pair, terminated},
};
use std::{
    collections::HashSet,
    env,
    error::Error,
    fs::{self, File},
    io::Write,
    path::Path,
};

fn string(input: &str) -> IResult<&str, &str> {
    let quote = "\"";
    delimited(tag(quote), is_not(quote), tag(quote)).parse(input)
}

fn emoji_pair(input: &str) -> IResult<&str, (&str, &str)> {
    separated_pair(
        alt((string, is_not(":"))),
        (tag(":"), multispace0()),
        string,
    )
    .parse(input)
}

fn object(input: &str) -> IResult<&str, Vec<(&str, &str)>> {
    delimited(
        (tag("{"), multispace0()),
        many0(terminated(emoji_pair, (tag(","), multispace0()))),
        tag("}"),
    )
    .parse(input)
}

fn parse(input: &str) -> IResult<&str, Vec<(&str, &str)>> {
    let needle = "export const replacements";

    preceded(
        (
            take_until(needle),
            tag(needle),
            multispace0(),
            tag("="),
            multispace0(),
        ),
        object,
    )
    .parse(input)
}

fn codegen(emoji_data: &[(&str, &str)]) -> String {
    let mut emoji_to_name_map = phf_codegen::Map::new();
    let mut name_to_emoji_map = phf_codegen::Map::new();

    let mut seen_names = HashSet::new();

    for (emoji, name) in emoji_data {
        if !seen_names.insert(name) {
            continue;
        }
        emoji_to_name_map.entry(emoji, format!(r##"r#"{name}"#"##));
        name_to_emoji_map.entry(name, format!(r##"r#"{emoji}"#"##));
    }

    format!(
        r#"
        static EMOJI_TO_NAME: phf::Map<&'static str, &'static str> = {0};
        static NAME_TO_EMOJI: phf::Map<&'static str, &'static str> = {1};
    "#,
        emoji_to_name_map.build(),
        name_to_emoji_map.build()
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    // The OS is my garbage collector
    let emoji_data = fs::read_to_string("./data/emoji.js")?.leak();
    let (_, emoji_data) = parse(emoji_data)?;

    let out_path = Path::new(&env::var("OUT_DIR")?).join("emoji.rs");
    let mut file = File::create(&out_path)?;

    file.write_all(codegen(&emoji_data).as_bytes())?;

    Ok(())
}
