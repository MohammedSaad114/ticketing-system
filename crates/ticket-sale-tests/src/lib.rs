//! Testing infrastructure

#![warn(missing_docs)]

use eyre::{eyre, Result};
use project_settings::ProjectSettings;

mod api;
mod project_settings;
pub use api::{Api, ApiResponse, RequestOptions, Reservation, SessionState, UserSession};

/// Run configuration: Which implementation (Rust/Java) to test
#[derive(Clone, Debug)]
pub enum RunCfg {
    /// Use the implementation of the `ticket-sale-rocket` crate
    RustNative,
    /// Use the Java native interface and the given Jar path
    JavaNative(String),
}

/// Builder for [`TestCtx`]
pub struct TestCtxBuilder {
    /// Whether to run the bonus implementation
    pub bonus: bool,
    /// Initial ticket number
    pub tickets: u64,
    /// Count of balancer threads
    pub balancer_threads: u16,
    /// Ticket reservation timeout in seconds
    pub reservation_timeout: u32,
    /// Time in seconds the estimator takes to contact all servers
    pub estimator_roundtrip_time: u32,

    /// Whether to enable Java assertions (default: true)
    pub assertions: bool,

    /// Which implementation to launch
    pub run_cfg: RunCfg,
}

impl TestCtxBuilder {
    /// Create a new test context builder initialized with environment defaults
    pub fn from_env() -> Result<Self> {
        use project_settings::ProgrammingLanguage::*;

        let settings = ProjectSettings::load()?;

        let run_cfg = match settings.language {
            Rust => RunCfg::RustNative,
            Java => {
                let Some(path) = settings.java.jar.to_str() else {
                    return Err(eyre!("java.jar setting contains invalid UTF-8"));
                };
                RunCfg::JavaNative(path.into())
            }
        };

        Ok(TestCtxBuilder {
            bonus: settings.bonus_implemented && settings.test_bonus,
            tickets: 1_000,
            balancer_threads: 2,
            reservation_timeout: 10,
            estimator_roundtrip_time: 10,
            assertions: true,
            run_cfg,
        })
    }

    /// Set the number of initially available tickets
    pub fn with_tickets(mut self, tickets: u64) -> Self {
        self.tickets = tickets;
        self
    }

    /// Set the number of balancer threads to use
    pub fn with_balancer_threads(mut self, threads: u16) -> Self {
        assert_ne!(threads, 0);
        self.balancer_threads = threads;
        self
    }

    /// Set the ticket reservation timeout (in seconds)
    pub fn with_reservation_timeout(mut self, timeout: u32) -> Self {
        self.reservation_timeout = timeout;
        self
    }

    /// Set the time the estimator takes to contact all servers (in seconds)
    pub fn with_estimator_roundtrip_time(mut self, time: u32) -> Self {
        self.estimator_roundtrip_time = time;
        self
    }

    /// Try to disable assertions
    pub fn disable_assertions(mut self) -> Self {
        self.assertions = false;
        self
    }

    /// Get the [`ticket_sale_core::Config`] for launching the ticket sales system
    fn config(&self) -> ticket_sale_core::Config {
        ticket_sale_core::Config {
            tickets: self.tickets as u32,
            timeout: self.reservation_timeout,
            initial_servers: 2,
            estimator_roundtrip_time: self.estimator_roundtrip_time,
            bonus: self.bonus,
        }
    }

    /// Build the test context
    pub async fn build(self) -> Result<TestCtx> {
        let config = self.config();
        let (balancer, api) = match self.run_cfg {
            RunCfg::RustNative => {
                let (balancer, api) = api::mock::start(self.balancer_threads, config).await;
                (Balancer::MockBalancer(balancer), api)
            }
            RunCfg::JavaNative(exec) => {
                let (balancer, api) =
                    api::jni::start(self.balancer_threads, &config, &exec, self.assertions).await?;
                (Balancer::JniBalancer(balancer), api)
            }
        };

        Ok(TestCtx {
            api,
            balancer,
            bonus: self.bonus,
            tickets: self.tickets,
            balancer_threads: self.balancer_threads,
            reservation_timeout: self.reservation_timeout,
            drop_bomb: DropBomb,
        })
    }
}

enum Balancer {
    MockBalancer(api::mock::MockBalancer),
    JniBalancer(api::jni::JniBalancer),
}

/// Test context
pub struct TestCtx {
    /// API allowing to interact with the ticket sales system
    pub api: Api,
    balancer: Balancer,
    /// Whether the bonus implementation is selected
    pub bonus: bool,
    /// Initial number of tickets
    pub tickets: u64,
    /// Number of balancer threads
    pub balancer_threads: u16,
    /// Reservation timeout
    pub reservation_timeout: u32,

    drop_bomb: DropBomb,
}

impl TestCtx {
    /// Shut down the ticket sales system and finish the test
    pub async fn finish(self) {
        std::mem::forget(self.drop_bomb);
        drop(self.api);
        match self.balancer {
            Balancer::MockBalancer(b) => b.shutdown().await,
            Balancer::JniBalancer(b) => b.shutdown().await,
        }
    }
}

struct DropBomb;

impl Drop for DropBomb {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            eprintln!("@TestAuthor: You should call `ctx.finish().await` to shut the ticket sales system down");
        }
    }
}
