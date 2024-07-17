set positional-arguments

# Print available recipes
help:
    @just --list

lang := `python3 -c 'import tomllib;print(tomllib.loads(open("project.toml","r",encoding="utf-8-sig").read())["language"])' || echo java`
export CP_LANGUAGE := lang

build_command := if lang != "rust" { "./gradlew jar" } else { "cargo build" }

# Build the project
build:
    {{build_command}}


build_jar := if lang != "rust" { "./gradlew jar" } else { "" }
run_command := if lang == "rust" {
    "cargo run -p ticket-sale-server --"
} else {
    "java -ea -jar out/cli.jar"
}

# Build and run the project for interaction with Cust-O-Matic 3000™
run *FLAGS:
    {{build_jar}}
    {{run_command}} "$@"


java_tests := if lang != "rust" { "./gradlew test" } else { "" }
release := if lang == "rust" { "" } else { "1" }
release_flag := if release == "" { "" } else if release == "0" { "" } else { "--release" }

# Run the tests in crates/ticket-sale-tests/tests (use `just release=1 test` to benchmark your Rust implementation)
test:
    {{java_tests}}
    {{build_jar}}
    cargo test -p ticket-sale-tests --tests {{release_flag}} -- --show-output

no_lint_command := if lang != "rust" { "echo 'No lint command defined for Java'" } else { "" }

# Run clippy on Rust code
lint:
    @{{no_lint_command}}
    cargo clippy

doc_command := if lang == "rust" { "cargo doc -p ticket-sale-rocket --document-private-items" } else { "./gradlew javadoc" }
doc_path := if lang == "rust" {
    "target"/"doc"/"ticket_sale_rocket"/"index.html"
} else {
    "build"/"docs"/"javadoc"/"index.html"
}

# Generate API documentation
doc:
    {{doc_command}}

open_command := if os() == "macos" { "open" } else if os() == "windows" { "start" } else { "xdg-open" }

# Generate API documentation and open it in your web browser
doc-open: doc
    {{open_command}} {{doc_path}}

# Run CSpell
spellcheck:
    cspell --quiet --unique --gitignore --dot --cache '**'

# Get the detected programming language
lang:
    @echo {{lang}}
