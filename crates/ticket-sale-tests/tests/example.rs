use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};

mod util;

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(20_000)] // Test timeout in ms
async fn test_example() -> Result<()> {
    // Create a test context with 1000 initially available tickets
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(1_000)
        .build()
        .await?;

    // Create a new user session
    let mut session = ctx.api.create_user_session(None);

    // Buy one ticket
    match session.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            panic!("It must be possible to reserve a ticket.")
        }
        Reservation::Reserved(ticket_id) => {
            assert!(
                session.buy_ticket(ticket_id).await?.result.is_ok(),
                "It must be possible to buy the ticket that we just reserved.",
            );
        }
    }

    // Finish the test
    ctx.finish().await;
    Ok(())
}
