use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};

mod util;
#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(20_000)] // Test timeout in ms
async fn test_example() -> Result<()> {
    // Create a test context with 10 initially available tickets
    let ctx = TestCtxBuilder::from_env()?.with_tickets(10).build().await?;

    // Log the initial server state or available tickets
    println!("Test context initialized. Attempting to reserve a ticket.");

    // Create a new user session
    let mut session = ctx.api.create_user_session(None);

    match session.reserve_ticket().await?.result? {
        Reservation::SoldOut => {
            // Log if reservation fails unexpectedly
            println!("Failed to reserve ticket: SoldOut");
            panic!("It must be possible to reserve a ticket.");
        }
        Reservation::Reserved(t) => {
            println!("Reserved ticket: {}", t);
            let wrong_number;
            if t == 5 {
                // very stupid way to ensure we always buy a wrong ticket
                wrong_number = 6;
            } else {
                wrong_number = 5;
            }
            if session.buy_ticket(wrong_number).await?.result.is_ok() {
                panic!("Should not be able to buy different ticket than reserved");
            }
        }
    }

    // Finish the test
    ctx.finish().await;
    Ok(())
}
