use std::collections::{HashMap, HashSet};

use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder, UserSession};
use uuid::Uuid;

mod util;

// Test, how good your implementation is at handling the reservation timeouts when servers
// start terminating while still having pending reservations :)

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(10_000)] // Test timeout in ms
async fn timeout_efficiency() -> Result<()> {
    let ticket_amount = 10000; // Change this if required
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(ticket_amount)
        .with_reservation_timeout(50)
        .build()
        .await?;

    let target_server_amount_to_test = 20; // The more your implementation can handle the more efficient it checks timeouts
                                           // Start with many servers
    ctx.api
        .post_num_servers(target_server_amount_to_test)
        .await?;
    // Create a new user session
    let users_amount = 10000; // The more your implementation can handle the more efficient it checks timeouts
    let mut tickets_sold = HashSet::<u64>::new();
    let mut reservations = HashMap::<Uuid, u64>::new();
    let mut sessions = Vec::<UserSession>::new();
    for _ in 0..users_amount {
        let session = ctx.api.create_user_session(None);
        sessions.push(session);
    }

    // Reserve users_amount many tickets
    for session in sessions.iter_mut() {
        match session.reserve_ticket().await?.result? {
            Reservation::SoldOut => {
                panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                reservations.insert(session.customer_id, ticket_id);
            }
        }
    }

    // Terminate all servers except one -> servers enter terminating mode and therefore
    // "critical section"
    ctx.api.post_num_servers(1).await?;

    // Buy the reserved tickets
    for session in sessions.iter_mut() {
        if session
            .buy_ticket(*reservations.get(&session.customer_id).unwrap())
            .await?
            .result
            .is_err()
        {
            panic!("We just reserved the ticket and should be able to buy it");
        } else {
            tickets_sold.insert(*reservations.get(&session.customer_id).unwrap());
            reservations.remove(&session.customer_id);
        }
    }

    // Every user should have been able to buy a ticket
    assert!(tickets_sold.len() == users_amount);

    // Finish the test
    ctx.finish().await;
    Ok(())
}
