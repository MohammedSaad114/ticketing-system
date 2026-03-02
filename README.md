# Selling Tickets Online (Swiftly)

A Concurrent Ticket Sale system created for the Concurrent Programming course at Saarland University lectured by [Prof. Dr.-Ing. Philipp Slusallek](https://depend.cs.uni-saarland.de/~hermanns/) during the Summer Term 2024

---

## Structure

The template is used for both Java and Rust. It is structured as follows:

- `project.toml`: Configuration file
- `crates/`: Rust code
  - `ticket-sale-core`: Infrastructure for working with requests
  - `ticket-sale-rocket`: Your implementation should go here
  - `ticket-sale-server`: HTTP server binary using your implementation
  - `ticket-sale-tests`: Automated integration tests (can be used for Java as well)

--- 

## Getting Started

First, you need to install VS Code and Rust. You will find further information here:

* https://code.visualstudio.com/
* https://rustup.rs

---

## Running the Project
- `cargo run -p ticket-sale-server` compiles and starts the ticket sales system.
- `cargo doc` generates API documentation. `cargo doc --open` additionally opens
  the generated documentation in a web browser.
- `cargo test -p ticket-sale-rocket` runs the unit tests.

To pass flags to the implementation,
e.g., to execute the slug implementation, append them after a `--`:
```sh
cargo run -p ticket-sale-server -- -slug
```


### Test Infrastructure

Our automated test infrastructure is written in Rust. 

Running the test suite is possible via
```sh
cargo test -p ticket-sale-tests --tests -- --show-output
```

If you want to test your implementation’s performance, you should build the
tests (as well as your project) in release mode. To this end, just add the
`--release` flag to the command above:
```sh
cargo test -p ticket-sale-tests --release --tests -- --show-output
```

## Rust-Specific Remarks

We use the following Rust libraries:

- [`parking_lot`](https://docs.rs/parking_lot/latest/parking_lot/index.html) for
  mutexes without lock poisoning
- [`rand`](https://docs.rs/rand/latest/rand/index.html) to get random values
- [`uuid`](https://docs.rs/uuid/latest/uuid/index.html) for server and customer
  ids
