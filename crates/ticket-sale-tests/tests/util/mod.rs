use std::collections::HashSet;

use eyre::Result;
use ticket_sale_tests::TestCtx;
use uuid::Uuid;

/// Scales the number of servers and tests that scaling works.
#[allow(unused)]
pub async fn scale_to(ctx: &TestCtx, num_servers: usize) -> Result<HashSet<Uuid>> {
    ctx.api.post_num_servers(num_servers).await?;
    assert_eq!(
        ctx.api.get_num_servers().await?.result?,
        num_servers,
        "After scaling to {num_servers} servers, the number of servers must be {num_servers}."
    );
    let servers = HashSet::from_iter(ctx.api.get_servers().await?.result?);
    assert_eq!(
      servers.len(), num_servers,
      "After scaling to {num_servers} servers, `get_servers` must return {num_servers} server ids."
  );
    Ok(servers)
}
