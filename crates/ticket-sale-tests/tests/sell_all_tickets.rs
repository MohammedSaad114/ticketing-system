use std::collections::HashSet;

use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};

mod util;

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(20_000)] // Test timeout in ms
async fn test_example() -> Result<()> {
    // Create a test context with 10 initially available tickets
    let ctx = TestCtxBuilder::from_env()?.with_tickets(10).build().await?;

    // Create a new user session
    let mut session = ctx.api.create_user_session(None);
    let mut session2 = ctx.api.create_user_session(None);
    session.api.post_num_servers(1).await?;
    let mut tickets = HashSet::<u64>::new();

    //Buy all tickets
    for _ in 0..5 {
        match session.reserve_ticket().await?.result? {
            Reservation::SoldOut => {
                panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                assert!(
                    session.buy_ticket(ticket_id).await?.result.is_ok(),
                    "It must be possible to buy the ticket that we just reserved.",
                );
                tickets.insert(ticket_id);
            }
        }

        match session2.reserve_ticket().await?.result? {
            Reservation::SoldOut => {
                panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                assert!(
                    session2.buy_ticket(ticket_id).await?.result.is_ok(),
                    "It must be possible to buy the ticket that we just reserved.",
                );
                tickets.insert(ticket_id);
            }
        }
    }

    assert!(
        tickets.len() == 10,
        "All tickets must be sold out and none double"
    );

    match session.reserve_ticket().await?.result? {
        Reservation::SoldOut => {}
        Reservation::Reserved(_) => {
            panic!("Should be sold out");
        }
    }

    // Finish the test
    ctx.finish().await;
    Ok(())
}
