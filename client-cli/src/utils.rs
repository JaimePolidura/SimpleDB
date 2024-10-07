use std::time::Duration;

pub fn duration_to_string(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let millis = duration.as_millis();
    format!("{:02}.{:02}s", seconds, millis / 10)
}