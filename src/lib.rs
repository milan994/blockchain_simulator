//! Server broadcasts blockchain Blocks on the WebSocket.
//! URL used: 127.0.0.1:7879
//! All implementation is in one file (this one) since it is a small exercise.
use core::time;
use axum::{
    extract::ws::{WebSocketUpgrade, WebSocket, Message},
    response::Response,
    routing::any,
    Router,
};
use sha2::{Sha256, Digest};
use tokio::{sync::broadcast, task::JoinHandle};
use serde;
use axum_server;
use std::time::{SystemTime, UNIX_EPOCH};


#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct Block {                  // simulated blockchain block
    index: u64,                 // position in the chain
    timestamp: u128,            // time when block is made ------ update using real timestamp crate
    transactions: Vec<String>,  // transactions
    nonce: u64,                 // used for proof-of-work
    hash: String,               // hash which will be caluculated
    previous_hash: String,      // hash of previous block
}

impl Block {
    fn new () -> Block {
        Block{
            index: 0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            transactions: vec![String::from("No transtactions")],
            nonce: 0,
            hash: "0".to_string(),
            previous_hash: String::from("0"),
        }
    }

    fn compute_hash(&self) -> String {
        let mut hasher = Sha256::new();
        let block_data = format!("{}{}{:?}{}{}",
            self.index, 
            self.timestamp, 
            self.transactions, 
            self.nonce, 
            self.previous_hash);

        hasher.update(block_data.as_bytes());

        format!("{:x}", hasher.finalize())

    }
}

#[derive(Clone)]
struct AppState {                   // name chosen basd on example from axum crate doc
    tx: broadcast::Sender<Block>,   // used to send Block
}

/// Main function which is called by main crate, it is starting point of our project and
/// it wraps the entire project.
///
/// # Panics
///
/// The `fn_blockchain_simulator_main` function will panic if parsing of URL address fails and 
/// if binding to address fails.
pub async fn fn_blockchain_simulator_main() -> JoinHandle<()> {
    let (tx, _) = broadcast::channel::<Block>(100); // capacity 100 just for the exercise
    let state = AppState {tx: tx.clone()};

    let state_clone = state.clone();
    let app = Router::new()
        .route("/ws", any(move |ws| handler(ws, state_clone)));

    let task_join_handle = block_generator(state.clone()).await;

    let addr = "127.0.0.1:7879".parse().unwrap();

    tracing::info!("blockchain_simulator server running at ws://127.0.0.1:7879");

    let server_handle  =  tokio::spawn(async move {
        axum_server::Server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    });

    match task_join_handle.await {
        Ok(_) => tracing::info!("Successful creation of a blockchain!\n"),
        Err(err) => tracing::error!("Error with task join handle: {err}\n"),        
    }

    server_handle
}

/// When the HTTP client successfully upgrades to a WebSocket, 
/// call this async function and give it the socket.
/// It registers a callback ('handle_socket') that is run asynchronously when the upgrade completes.
/// 
/// The 'ws' is used to Upgrade connection to WebSocket.
/// The 'state' is used to forward it further to the callback in order to create Receiver
/// on the Transmiter which transmits 'Block'.
async fn handler(ws: WebSocketUpgrade, state: AppState) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// This function is callback which is called during Upgrade of connection to WebSocket.
/// 
/// 'socket' is WebSocket used to send data to all clients./// 
/// 'state' is used to create the Receiver which is going to receive newly created 'Block'.
async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut rx = state.tx.subscribe();
    while let Ok(block) = rx.recv().await {

        // comparing to format! json format looks cleaner
        if socket.send(Message::Text(serde_json::to_string(&block)
                                                        .unwrap().into()))
                                                        .await
                                                        .is_err() {
                                                                    break;
        }
    }
}

/// Used to create new 'Block' and to send it over the channel.
/// 
/// 'state' is used to transmit newly created 'Block'.
async fn block_generator (state: AppState) -> JoinHandle<()> {
    let mut genesis_block: bool = true;
    let mut previous_hash: String = String::from("0");
    let mut block_index: u64 = 0;    

    tracing::debug!("Entering block_generator\n");

    let task_handle = tokio::spawn(async move {
        tracing::debug!("Entering tokio async task");
        loop {
            tracing::debug!("Entering loop\n");

            tokio::time::sleep(time::Duration::from_secs(3)).await;

            let mut block = Block::new();
    
            // first (Genesis block), do not increment it's index and leave it's block.previous_hash to zero
            if false == genesis_block {
                block.index = block_index;
                block.previous_hash = previous_hash;
                block.hash = block.compute_hash();
                previous_hash = block.hash.clone(); // possible performance impact because of .clone(), FIXME
                block_index += 1;
    
                tracing::debug!("Block:\n{:#?}", block);
            } else {
                genesis_block = false;
                block.hash = block.compute_hash();
                previous_hash = block.hash.clone(); // possible performance impact because of .clone(), FIXME
                block_index += 1;
    
                tracing::debug!("Genesis Block:\n{:#?}", block);
            }

            match state.tx.send(block.clone()) {
                Ok(_) => tracing::debug!("Sent: {:#?}", block),
                Err(err) => tracing::debug!("Error sending block: {}", err),
            }    
        }
    });

    task_handle     
}
