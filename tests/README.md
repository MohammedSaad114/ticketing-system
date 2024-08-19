# Community Test collection for Rust
## How to stay synchronized
A nice and easy way to stay up to date with this repo is to replace the current tests directory with a git submodule to this repo:
- First save all tests of yours that are not in this repo
- then delete the current tests directory (The one at crates/ticket-sale-tests/tests)
- Get the submodule with `git submodule add https://dgit.cs.uni-saarland.de/np/2024/np-ss24-project-community-tests.git tests`
(This expects you to execute the command from the `crates/ticket-sale-tests` directory)
- If you want your CI to actually run the tests (otherwise you can simply run them locally) you also have to add this at the beginning of `.gitlab-ci.yml`
    ```
    variables:
      GIT_SUBMODULE_STRATEGY: recursive
    
    ``` 

Now you can execute tests as usual (with `cargo t`) and stay up to date either through your IDE (vs code simply offers it under the git tab with the main repo) or by going into the tests directory and pulling
