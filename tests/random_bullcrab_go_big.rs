use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};
use core::panic;
use std::{collections::HashSet,  sync::Arc, thread, time::Duration};
use futures::executor::block_on;
mod util;
use tokio::time;
use nanorand::{Rng, WyRand, SeedableRng};

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(80_000)] // Test timeout in ms
async fn test_generate_data_for_profiler() -> Result<()> {
    // Create 200000 tickets
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(200000)
        .with_reservation_timeout(1)
        .build()
        .await?;
    let api = Arc::new(ctx);
    
    // Create 20 + 1 new user sessions
    
    let session = api.api.create_user_session(None);
    session.api.post_num_servers(20).await?;
    thread::scope(|s|
        {
                let mut threads = Vec::new();

    for i in 0_u64..20 {
    //Buy all tickets
    let api_new = Arc::clone(&api);
    // the 20 sessions try to reserve and buy 10000 tickets each
    let threading  = s.spawn(move ||
        
        {
        
        let mut user = api_new.api.create_user_session(None);
        let mut random = WyRand::new();
        random.reseed(i.to_le_bytes());
        let mut timeouts = 0;
        let mut tickets = HashSet::<u64>::new();
                for _ in 0..10000 {
        if random.generate_range(0..2) > 0 {
                        
                            assert!(
                            block_on(user.get_available_tickets()).unwrap().result.is_ok(),
                            "Could not receive number of tickets"
                        );
                        
                    }
                    else{
                    assert!(
                    block_on(user.abort_purchase(random.generate_range(1..5000))).unwrap().result.is_err(),
                        "Could abort a reservation we never did"

                )
                    }
        if let Ok(answer) = block_on(user.reserve_ticket()).unwrap().result { 
        match answer {
            Reservation::SoldOut => {
                            ()// panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                match random.generate_range(0..4){
                    0 =>  {assert!(
                    block_on(user.buy_ticket(ticket_id)).unwrap().result.is_ok(),
                    "It must be possible to buy the ticket that we just reserved.",
                );
                tickets.insert(ticket_id);},
                    1 => {assert!(
                    block_on(user.buy_ticket(random.generate_range(0..ticket_id))).unwrap().result.is_err(),
                    "Could buy a different ticket than we ordered"
                );
                                    assert!(
                                    block_on(user.buy_ticket(ticket_id)).unwrap().result.is_ok(),
                                        "Could not buy our ticket after trying to buy a different one",
                                )}
                    2 => {
                                        assert!(
                                        block_on(user.reserve_ticket()).unwrap().result.is_err(),
                                        "Could reserve another ticket"
                                    );
                                    if random.generate_range(0..50) <1 && timeouts < 10{
                                            timeouts += 1;
                                            thread::sleep(Duration::from_secs(2));
                                            assert!(
                                            block_on(user.buy_ticket(ticket_id)).unwrap().result.is_err(),
                                            "Could buy ticket after timing out"
                                        )

                                        }
                                    else {
                                            assert!(
                                            block_on(user.buy_ticket(ticket_id)).unwrap().result.is_ok(),
                                            "Could not buy ticket after trying to reserve a different one",
                                        )
                                        }
                                    }
                    _ => ()
                                }
               
            }
        }
}
    }
            });
        
    threads.push(threading);

    }
    let mut random = WyRand::new();
    // while there are threads that have not yet terminated, randomly adjust the number of servers
    // 20 times per second
    for thread_running in threads {
        while !thread_running.is_finished() {
            thread::sleep(Duration::from_millis(100));
            block_on(session.api.post_num_servers(random.generate_range(1_usize..21)));
        }
    } }
);
    session.api.post_num_servers(1).await?;
    time::sleep(Duration::from_secs(2)).await;
    println!("{} tickets left", session.api.get_num_servers().await?.result?);
    /* match session.reserve_ticket().await?.result? {
         Reservation::SoldOut => {
            
        }
        Reservation::Reserved(_) => {
            //panic!("Should be sold out");
            ()
        }
    }*/


    // Finish the test
    
    
    Arc::into_inner(api).unwrap().finish().await;
    Ok(())
}
