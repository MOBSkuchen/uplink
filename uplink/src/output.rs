use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use rand::seq::IndexedRandom;
use std::time::Duration;

use crate::error::Result;

const SPINNER_LOAD: &str = "←↖↑↗→↘↓↙";
const SPINNER_PACK: &str = "◐◓◑◒";

pub fn step(msg: impl AsRef<str>) {
    println!("{}{}", style("> ").cyan().bold(), style(msg.as_ref()).bold());
}

pub fn info(label: &str, value: impl AsRef<str>) {
    println!("  {} {}", style(format!("{}:", label)).dim(), value.as_ref());
}

pub enum RandomMessage {
    ServerProcessing,
    ClientProcessing,
    Uploading,
    Downloading,
    Packing,
    Unpacking,
    Compressing,
    Decompressing,
    WaitingOnServer,
}

impl RandomMessage {
    fn random_message(&self) -> &'static str {
        let mut rng = rand::rng();

        let choices = match self {
            RandomMessage::ServerProcessing => &[
                "conjuring",
                "brewing",
                "percolating",
                "ruminating",
                "divining",
            ],
            RandomMessage::ClientProcessing => &[
                "tinkering",
                "juggling",
                "doodling",
                "weaving",
                "whittling",
            ],
            RandomMessage::Uploading => &[
                "flinging",
                "catapulting",
                "levitating",
                "ascending",
                "whooshing",
            ],
            RandomMessage::Downloading => &[
                "summoning",
                "snatching",
                "materializing",
                "descending",
                "swooping",
            ],
            RandomMessage::Packing => &[
                "swaddling",
                "stuffing",
                "nesting",
                "bundling",
                "tucking",
            ],
            RandomMessage::Unpacking => &[
                "blooming",
                "hatching",
                "unfurling",
                "sprouting",
                "spilling",
            ],
            RandomMessage::Compressing => &[
                "squeezing",
                "shrinking",
                "squashing",
                "smushing",
                "deflating",
            ],
            RandomMessage::Decompressing => &[
                "stretching",
                "popping",
                "inflating",
                "fluffing",
                "uncoiling",
            ],
            RandomMessage::WaitingOnServer => &[
                "snoozing",
                "daydreaming",
                "twiddling",
                "yearning",
                "pining",
            ],
        };

        choices.choose(&mut rng).copied().unwrap_or("thinking")
    }

    fn spinner(&self) -> &'static str {
        match self {
            RandomMessage::ServerProcessing => "⠄⠆⠇⠋⠙⠸⠰⠠⠰⠸⠙⠋⠇⠆",
            RandomMessage::ClientProcessing => "⠁⠁⠉⠙⠚⠒⠂⠂⠒⠲⠴⠤⠄⠄⠤⠠⠠⠤⠦⠖⠒⠐⠐⠒⠓⠋⠉⠈⠈",
            RandomMessage::Uploading => SPINNER_LOAD,
            RandomMessage::Downloading => SPINNER_LOAD,
            RandomMessage::Packing => SPINNER_PACK,
            RandomMessage::Unpacking => SPINNER_PACK,
            RandomMessage::Compressing => "┤┘┴└├┌┬┐",
            RandomMessage::Decompressing => "▖▘▝▗",
            RandomMessage::WaitingOnServer => "⠂-–—–-",
        }
    }
}

pub fn progress(len: u64, kind: RandomMessage) -> Result<ProgressBar> {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  {spinner:.cyan} {msg:<12.bold.cyan} [{bar:32.cyan/blue}] \
                 {bytes:>10.green}/{total_bytes:.green} {percent:>3.yellow}% \
                 {bytes_per_sec:.magenta} eta {eta:.dim}",
            )?
            .tick_chars(kind.spinner())
            .progress_chars("█▉▊▋▌▍▎▏ "),
    );
    pb.set_message(kind.random_message());
    pb.enable_steady_tick(Duration::from_millis(80));
    Ok(pb)
}

pub fn spinner(kind: RandomMessage) -> Result<ProgressBar> {
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("  {spinner:.cyan} {msg:<12.bold.cyan}")?
            .tick_chars(kind.spinner()),
    );
    pb.set_message(kind.random_message());
    pb.enable_steady_tick(Duration::from_millis(80));
    Ok(pb)
}
