//! Server broadcasts blockchain Blocks on the WebSocket.
//! URL used: 127.0.0.1:7879
//! All implementation is in one file (this one) since it is a small exercise.
use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing::any,
};
use core::time;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{sync::broadcast, task::JoinHandle};

/// Simulated blockchain block.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone)]
struct Block {
    /// Position in the chain
    index: u64,
    /// Time when block is made ------ update using real timestamp crate
    timestamp: u128,
    /// Transactions
    transactions: Vec<String>,
    /// Used for proof-of-work
    nonce: u64,
    /// Hash which will be calculated
    hash: String,
    /// Hash of previous block
    previous_hash: String,
}

// Cest pattern kad imas neku strukturu sa mnogo field-ova je ovaj builder kao. Mislim da je
// jednostavno, mozes samo procitati kod.
#[derive(Debug, Default)]
struct BlockBuilder {
    index: u64,
    timestamp: Option<u128>,
    transactions: Option<Vec<String>>,
    nonce: Option<u64>,
    hash: Option<String>,
    previous_hash: Option<String>,
}

impl BlockBuilder {
    fn new(index: u64) -> Self {
        Self {
            index,
            // Default trait je jako koristan, isto pogotovo ako struct ima puno fieldova. Ovo
            // ispod znaci, sve ostale fieldove stavi da budu default. Uopsteno ova sintaksa znaci
            // - fieldove koje sam naveo stavi da budu ta i ta vrednost, sve posle `..` stavi da
            // budu iste vrednosti kao ovaj drugi struct (istog tipa). Primer:
            //
            // let a: SomeType = ...;
            // let b = SomeType {
            //     field1: 1,
            //     ..a,
            // };
            ..Default::default()
        }
    }

    fn new_genesis() -> Self {
        Self::new(0)
    }

    // Primeti kako neke funkcije dodaju field kroz parametar a neke, poput ove, ne primaju
    // parametar nego samo racunaju vrednost fielda.
    fn with_timestamp(mut self) -> Self {
        self.timestamp = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis(),
        );
        self
    }

    fn with_transactions(mut self, transactions: Vec<String>) -> Self {
        self.transactions = Some(transactions);
        self
    }

    fn with_nonce(mut self, nonce: u64) -> Self {
        self.nonce = Some(nonce);
        self
    }

    fn with_previous_hash(mut self, previous_hash: String) -> Self {
        self.previous_hash = Some(previous_hash);
        self
    }

    // Ova funkcija ima nekoliko stvari u sebi koje nije lose skontati.
    fn with_hash(mut self) -> Self {
        let mut hasher = Sha256::new();

        // 1. `to_le_bytes` / `from_le_bytes` i njihove `be` verzije
        hasher.update(self.index.to_le_bytes());
        // 2. `unwrap_or_default` i ostali `kombinatori` nad `Option<T> i Result<T>`
        hasher.update(self.nonce.unwrap_or_default().to_le_bytes());
        // 3. `as_ref / as_mut` nad option-om kad ne zelis da move-ujes T iz Option<T>
        hasher.update(self.previous_hash.as_ref().expect("Missing previous_hash"));
        hasher.update(
            self.timestamp
                .as_ref()
                .expect("Missing timestamp")
                .to_le_bytes(),
        );
        let transactions = self.transactions.unwrap_or_default();
        hasher.update(transactions.join(",").as_bytes());
        self.transactions = Some(transactions);

        self.hash = Some(format!("{:x}", hasher.finalize()));
        self
    }

    fn build(self) -> Block {
        Block {
            index: self.index,
            timestamp: self.timestamp.expect("Missing timestamp"),
            transactions: self.transactions.unwrap_or_default(),
            nonce: self.nonce.unwrap_or_default(),
            hash: self.hash.expect("Missing hash"),
            previous_hash: self.previous_hash.expect("Missing previous_hash"),
        }
    }
}

#[derive(Clone)]
struct AppState {
    // name chosen basd on example from axum crate doc
    tx: broadcast::Sender<Block>, // used to send Block
}

/// Main function which is called by main crate, it is starting point of our project and
/// it wraps the entire project.
///
/// # Panics
///
/// The `fn_blockchain_simulator_main` function will panic if parsing of URL address fails and
/// if binding to address fails.
#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel::<Block>(100); // capacity 100 just for the exercise
    let state = AppState { tx: tx.clone() };

    let state_clone = state.clone();
    let app = Router::new().route("/ws", any(move |ws| handler(ws, state_clone)));

    let task_join_handle = block_generator(state.clone()).await;

    let addr = "127.0.0.1:7879".parse().unwrap();

    tracing::info!("blockchain_simulator server running at ws://127.0.0.1:7879");

    axum_server::Server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    match task_join_handle.await {
        Ok(_) => println!("Successful creation of a blockchain!\n"),
        Err(err) => println!("Error: {err}\n"),
    }
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
        if socket
            .send(Message::Text(serde_json::to_string(&block).unwrap().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Used to create new 'Block' and to send it over the channel.
///
/// 'state' is used to transmit newly created 'Block'.
async fn block_generator(state: AppState) -> JoinHandle<()> {
    let mut is_genesis: bool = true;
    let mut previous_hash: String = String::from("0");
    let mut block_index: u64 = 0;

    tracing::debug!("Entering block_generator\n");

    tokio::spawn(async move {
        tracing::debug!("Entering tokio async task");
        loop {
            tracing::debug!("Entering loop\n");

            tokio::time::sleep(time::Duration::from_secs(3)).await;

            // first (Genesis block), do not increment it's index and leave it's block.previous_hash to zero
            let block = if !is_genesis {
                let block = BlockBuilder::new(block_index)
                    .with_nonce(0)
                    .with_previous_hash(previous_hash)
                    .with_timestamp()
                    .with_transactions(vec![])
                    .with_hash()
                    .build();
                previous_hash = block.hash.clone(); // possible performance impact because of .clone(), FIXME
                block_index += 1;

                tracing::debug!("Block:\n{:#?}", block);

                block
            } else {
                is_genesis = false;
                let block = BlockBuilder::new_genesis()
                    .with_nonce(0)
                    .with_previous_hash(previous_hash)
                    .with_timestamp()
                    .with_transactions(vec![])
                    .with_hash()
                    .build();
                previous_hash = block.hash.clone(); // possible performance impact because of .clone(), FIXME
                block_index += 1;
                tracing::debug!("Genesis Block:\n{:#?}", block);

                block
            };

            match state.tx.send(block.clone()) {
                Ok(_) => tracing::debug!("Sent: {:#?}", block),
                Err(err) => tracing::debug!("Error sending block: {}", err),
            }
        }
    })
}
