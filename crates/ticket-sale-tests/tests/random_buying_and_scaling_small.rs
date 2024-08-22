use eyre::Result;
use ticket_sale_tests::{Reservation, TestCtxBuilder};
use std::{collections::HashSet,  sync::Arc, thread, time::Duration};
use futures::executor::block_on;
mod util;
use tokio::time;
use nanorand::{Rng, WyRand};

#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(80_000)] // Test timeout in ms
async fn test_generate_data_for_profiler() -> Result<()> {
    // Create 200000 tickets
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(20000)
        .build()
        .await?;
    let api = Arc::new(ctx);
    
    // Create 20 + 1 new user sessions
    
    let session = api.api.create_user_session(None);
    session.api.post_num_servers(20).await?;
    let mut threads = Vec::new();
    for _ in 0..20 {
    //Buy all tickets
    let api_new = Arc::clone(&api);
    // the 20 sessions try to reserve and buy 10000 tickets each
    let threading  = thread::spawn(move ||
        
        {
        
        let mut user = api_new.api.create_user_session(None);

        let mut tickets = HashSet::<u64>::new();
                for _ in 0..10000 {
        if let Ok(answer) = block_on(user.reserve_ticket()).unwrap().result { 
        match answer {
            Reservation::SoldOut => {
                            ()// panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                assert!(
                    block_on(user.buy_ticket(ticket_id)).unwrap().result.is_ok(),
                    "It must be possible to buy the ticket that we just reserved.",
                );
                tickets.insert(ticket_id);
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
            time::sleep(Duration::from_millis(100)).await;
            session.api.post_num_servers(random.generate_range(1_usize..21)).await?;
        }
    } 
    
    session.api.post_num_servers(1).await?;
    time::sleep(Duration::from_secs(1)).await;
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
