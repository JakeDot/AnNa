#[tokio::main]
async fn main() -> anyhow::Result<()> {
    anna_sync_server::run().await
}
