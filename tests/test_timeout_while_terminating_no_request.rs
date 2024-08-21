use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};
use std::collections::HashSet;
use tokio;
use std::time::Duration;

mod util;

/**!!!!!
* This expects your server to notice itself after a certain time to check its own reservations and deplete old ones
* Otherwise it will not work
* The estimator_roundtrip is made very short so this test will not take forever
!!!!! Disclaimer! I am currently failing this test so it might not be correct but is very similar to the other test_timeout which passes on my implementation and makes sense for me
*/

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(40_000)] // Test timeout in ms
async fn test_example() -> Result<()> {
    // Create a test context with 10 initially available tickets
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(20)
        .with_reservation_timeout(1)
        .with_estimator_roundtrip_time(1)
        .build()
        .await?;

    // Create a new user session
    let servers = ctx.api.get_servers().await?.result?;
    let mut session = ctx.api.create_user_session(Some(servers[0]));
    let mut session2 = ctx.api.create_user_session(Some(servers[1]));

    match session.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            panic!("It must be possible to reserve a ticket.")
        }
        Reservation::Reserved(_) => {
            
        }
    }

    match session2.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            panic!("It must be possible to reserve a ticket.")
        }
        Reservation::Reserved(_) => {
            
        }
    }


    session.api.post_num_servers(1).await?;
    tokio::time::sleep(Duration::from_secs(10)).await; //Let reservation expire and the server terminate

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
