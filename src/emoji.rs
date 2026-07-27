include!(concat!(env!("OUT_DIR"), "/emoji.rs"));

pub fn emoji_to_name(emoji: &str) -> Option<&'static str> {
    EMOJI_TO_NAME.get(emoji).map(|n| &**n)
}

pub fn name_to_emoji(name: &str) -> Option<&'static str> {
    NAME_TO_EMOJI.get(name).map(|n| &**n)
}
