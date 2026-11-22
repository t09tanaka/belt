fn main() {
    std::process::exit(belt::workspace::embedded::dispatch().unwrap_or_else(belt::cli::main));
}
