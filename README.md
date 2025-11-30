# Selling Tickets Online (Swiftly)

University project 2024 🚀


## Structure

The template is used for both Java and Rust. It is structured as follows:

- `project.toml`: Configuration file (team name, programming language, bonus …)
- `crates/`: Rust code
  - `ticket-sale-core`: Infrastructure for working with requests
  - `ticket-sale-rocket`: Your implementation should go here
  - `ticket-sale-server`: HTTP server binary using your implementation
  - `ticket-sale-tests`: Automated integration tests (can be used for Java as well)
- `src/`: Java code
  - `main/java/com/pseuco/cp24/`: Java source code of the project
      - `request`: Infrastructure for working with requests
      - `rocket`: Your implementation should go here
      - `slug`: A sequential reference implementation
  - `test`: Unit tests

We initialized the `project.toml` according to your language preferences, but in
case you want to switch the programming language, it is possible there. If you
implement the bonus, also enable the `bonus-implemented` setting there.


## Getting Started

A detailed description on how to install Java and Rust can be found in the
[forum](https://cp24.pseuco.com/t/how-to-project-in-vs-code/385).

We recommend you use a proper *Integrated Development Environment* (IDE) for
the project. A good option is [VS Code](https://code.visualstudio.com/). How to
set up the project in VS Code is documented in
[the same forum post](https://cp24.pseuco.com/t/how-to-project-in-vs-code/385).

Which IDE or editor you use is up to you. However, we only provide help for
VS Code. In case you use some other code editor, do not expect help.


## Running the Project

### Java ☕

We use [Gradle](https://gradle.org/) to build the project. You can interact with
Gradle from the terminal. The most important subcommands are:

- `./gradlew jar` builds the project as `out/cli.jar`.
- `./gradlew run` compiles and starts the ticket sales system. You can then use
  [Cust-O-Matic 3000™](https://missioncontrol.pseuco.com/#/cust-o-matic) for
  manual testing.
- `./gradlew javaDoc` builds the Javadoc in `build/docs/`.
- `./gradlew test` runs the unit tests

Once you have built the JAR file, you can also run it without Gradle and supply
the command line flags listed in the project description. For instance, the
following command will start the sequential slug implementation:
```
java -ea -jar out/cli.jar -slug
```

Running (and debugging) the project directly from VS Code without the command
line is also possible, see the
[forum post](https://cp24.pseuco.com/t/how-to-project-in-vs-code/385) for more
details.


### Rust 🦀

If you use Rust, Cargo is your friend:

- `cargo run -p ticket-sale-server` compiles and starts the ticket sales system.
  You can then use
  [Cust-O-Matic 3000™](https://missioncontrol.pseuco.com/#/cust-o-matic) for
  manual testing.
- `cargo doc` generates API documentation. `cargo doc --open` additionally opens
  the generated documentation in a web browser.
- `cargo test -p ticket-sale-rocket` runs the unit tests you write inside your
  implementation.

To pass flags (as listed in the project description) to the implementation,
e.g., to execute the slug implementation, append them after a `--`:
```sh
cargo run -p ticket-sale-server -- -slug
```


### Test Infrastructure

Our automated test infrastructure is written in Rust. To add tests, place them
in `crates/ticket-sale-tests/tests/`. We provide an example test in
`example.rs`. You may create other files in this directory. Every test file may
contain multiple test cases, which are Rust functions decorated with
`#[tokio::test]`. For a general description of how to write tests in Rust, you
may want to have a look at
[Chapter 11 of the Rust Book](https://doc.rust-lang.org/stable/book/ch11-00-testing.html).

Running the test suite is possible via
```sh
cargo test -p ticket-sale-tests --tests -- --show-output
```

If you use Java, please note that this will not build your Java project. You
need to run `./gradlew jar` beforehand. If you are using Rust, Cargo takes care
of recompiling the project.

If you want to test your implementation’s performance, you should build the
tests (as well as your project) in release mode. To this end, just add the
`--release` flag to the command above:
```sh
cargo test -p ticket-sale-tests --release --tests -- --show-output
```

All automated tests are also run in GitLab CI. Already now, you will be automatically notified via e-mail if the build fails. Later on, once you pass
the tests, you may also want to enable notifications on test results in case a
commit introduces a bug. To this end set the `allow_failure` flag in
`.gitlab-ci.yml` to false.


### For Convenience: A `justfile` 🎉

Some of the commands above are a bit lengthy and may be hard to remember, some
even depend on other commands. To make your life a bit easier, we provide a
[`justfile`](https://just.systems/man/en/) with a simpler interface. To install
`just` via Cargo, run `cargo install just`. A complete list of commands provided
by our `justfile` can be obtained via `just help`:

    Available recipes:
    build      # Build the project
    doc        # Generate API documentation
    doc-open   # Generate API documentation and open it in your web browser
    help       # Print available recipes
    lang       # Get the detected programming language
    lint       # Run clippy on Rust code
    run *FLAGS # Build and run the project for interaction with Cust-O-Matic 3000™
    spellcheck # Run CSpell
    test       # Run the tests in crates/ticket-sale-tests/tests (use `just release=1 test` to benchmark your Rust implementation)

For example, you can use `just run` to build and run your project for use with
Cust-O-Matic 3000™. You can pass the command line options specified in the
project description directly to `just run`, e.g., `just run -tickets 1337` to start the server with 1337 initially available tickets.

`just` can automatically detect the programming language you selected in
`project.toml`. However, this requires Python ≥ 3.11 to be installed and
available via `python3`. If this is not the case on your system, you can select
the language manually, for example: `just lang=rust test` or
`just lang=java test`.


## Rust-Specific Remarks

The Rust template roughly follows the structure of the Java template.

We use the following Rust libraries:

- [`parking_lot`](https://docs.rs/parking_lot/latest/parking_lot/index.html) for
  mutexes without lock poisoning
- [`rand`](https://docs.rs/rand/latest/rand/index.html) to get random values
- [`uuid`](https://docs.rs/uuid/latest/uuid/index.html) for server and customer
  ids

You may assume that the safe interfaces provided by these crates are sound
(i.e., no matter how you use them within Safe Rust, you will not cause data
races or other kinds of undefined behavior). You are allowed to add other crates
as dependencies, but you take full responsibility for soundness issues within
them and we may ask you questions in about them as part of the defense.
