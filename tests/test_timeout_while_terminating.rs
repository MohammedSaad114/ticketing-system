use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};
use std::collections::HashSet;
use tokio;
use std::time::Duration;

mod util;

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(40_000)] // Test timeout in ms
async fn test_example() -> Result<()> {
    // Create a test context with 10 initially available tickets
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(20)
        .with_reservation_timeout(2)
        .with_estimator_roundtrip_time(1)
        .build()
        .await?;

    // Create a new user session
    let servers = ctx.api.get_servers().await?.result?;
    let mut session = ctx.api.create_user_session(Some(servers[0]));
    let mut session2 = ctx.api.create_user_session(Some(servers[1]));
    let ticket;
    let ticket2;

    match session.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            panic!("It must be possible to reserve a ticket.")
        }
        Reservation::Reserved(ticket_id) => {
            //TODO also check timeout if no request is made?
            ticket = ticket_id;
            
        }
    }

    match session2.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            panic!("It must be possible to reserve a ticket.")
        }
        Reservation::Reserved(ticket_id) => {
            //TODO also check timeout if no request is made?
            ticket2 = ticket_id;
            
        }
    }


    session.api.post_num_servers(1).await?;
    tokio::time::sleep(Duration::from_secs(3)).await; //Let reservation expire
    let current_server = (session.api.get_servers().await?.result?)[0];
    if current_server == servers[0] {
        assert!(session.buy_ticket(ticket).await?.result.is_err(), "This ticket should have timed out");
        assert!(session2.buy_ticket(ticket2).await?.result.is_err(), "This ticket should have timed out");
    } else {
        assert!(session2.buy_ticket(ticket2).await?.result.is_err(), "This ticket should have timed out");
        assert!(session.buy_ticket(ticket).await?.result.is_err(), "This ticket should have timed out");
    }

    tokio::time::sleep(Duration::from_secs(1)).await; //Give server some time for shutdown
    session = ctx.api.create_user_session(None);
    session2 = ctx.api.create_user_session(None);

    let mut tickets = HashSet::<u64>::new();

    //Buy all tickets
    for _ in 0..10 {
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

    assert!(tickets.len() == 20, "All tickets must be sold out and none double");

    match session.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            
        }
        Reservation::Reserved(_) => {
            panic!("Should be sold out");
        }
    }


    // Finish the test
    ctx.finish().await;
    Ok(())
}
