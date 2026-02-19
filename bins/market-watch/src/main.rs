//! Market Watch - Real-time orderbook monitoring
//!
//! Modes:
//!   --slug <market-slug>     Monitor single market
//!   --token <token-id>       Monitor single token
//!   --top-markets <N>        Monitor top N liquid neg-risk markets (default: 100)
//!
//! Shows every price change with latency measurement

use chrono::Utc;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const GAMMA_API: &str = "https://gamma-api.polymarket.com";
const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Parser, Debug)]
#[command(name = "market-watch")]
#[command(about = "Real-time orderbook monitoring for Polymarket")]
struct Args {
    /// Market slug (e.g., 2026-fifa-world-cup-winner-595)
    #[arg(short, long)]
    slug: Option<String>,

    /// Single token ID to monitor
    #[arg(short, long)]
    token: Option<String>,

    /// Token name for display (used with --token)
    #[arg(long, default_value = "TOKEN")]
    token_name: String,

    /// Monitor top N liquid neg-risk markets
    #[arg(long)]
    top_markets: Option<usize>,

    /// Show all updates (not just price changes)
    #[arg(long, default_value = "false")]
    verbose: bool,

    /// Run for N seconds then exit (0 = forever)
    #[arg(long, default_value = "0")]
    duration: u64,

    /// Show raw WebSocket messages
    #[arg(long, default_value = "false")]
    raw: bool,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    id: Option<String>,
    title: Option<String>,
    slug: Option<String>,
    #[serde(rename = "negRiskMarketID")]
    neg_risk_market_id: Option<String>,
    #[serde(rename = "negRisk")]
    neg_risk: Option<bool>,
    markets: Option<Vec<GammaMarket>>,
    volume: Option<serde_json::Value>,
    liquidity: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    question: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<serde_json::Value>,
    #[serde(rename = "acceptingOrders")]
    accepting_orders: Option<bool>,
    closed: Option<bool>,
    volume: Option<String>,
}

#[derive(Debug, Clone)]
struct TokenInfo {
    market_title: String,
    outcome_name: String,
    token_type: String, // "YES" or "NO"
    market_idx: usize,
}

#[derive(Debug, Deserialize)]
struct WsMessage {
    market: Option<String>,
    price_changes: Option<Vec<WsChange>>,
}

/// Book snapshot event (comes as array)
#[derive(Debug, Deserialize)]
struct BookEvent {
    asset_id: Option<String>,
    bids: Option<Vec<BookLevel>>,
    asks: Option<Vec<BookLevel>>,
    event_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    price: Option<String>,
    size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WsChange {
    asset_id: Option<String>,
    price: Option<String>,
    side: Option<String>,
    size: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

#[derive(Debug, Clone)]
struct PriceState {
    best_ask: Option<Decimal>,
    best_bid: Option<Decimal>,
    last_update: Instant,
    update_count: u64,
}

impl Default for PriceState {
    fn default() -> Self {
        Self {
            best_ask: None,
            best_bid: None,
            last_update: Instant::now(),
            update_count: 0,
        }
    }
}

#[derive(Serialize)]
struct SubscribeMsg {
    #[serde(rename = "type")]
    msg_type: String,
    channel: String,
    assets_ids: Vec<String>,
}

fn format_price(p: Option<Decimal>) -> String {
    match p {
        Some(d) => format!("{:.2}¢", d * Decimal::from(100)),
        None => "---".to_string(),
    }
}

fn now_str() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

async fn fetch_top_neg_risk_markets(
    client: &reqwest::Client,
    limit: usize,
) -> anyhow::Result<Vec<GammaEvent>> {
    println!("① Fetching neg-risk markets from Gamma API...");
    let start = Instant::now();

    let mut all_events = Vec::new();
    let mut offset = 0;
    let page_size = 100;

    loop {
        let resp = client
            .get(format!("{}/events", GAMMA_API))
            .query(&[
                ("active", "true"),
                ("closed", "false"),
                ("limit", &page_size.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await?;

        let events: Vec<GammaEvent> = resp.json().await?;
        let count = events.len();

        // Filter for neg_risk markets with 2+ outcomes
        for event in events {
            let is_neg_risk = event.neg_risk.unwrap_or(false)
                || event.neg_risk_market_id.is_some();

            if !is_neg_risk {
                continue;
            }

            let markets = event.markets.as_ref();
            let tradeable_count = markets
                .map(|m| {
                    m.iter()
                        .filter(|mkt| {
                            !mkt.closed.unwrap_or(false)
                                && mkt.accepting_orders.unwrap_or(true)
                        })
                        .count()
                })
                .unwrap_or(0);

            if tradeable_count >= 2 {
                all_events.push(event);
            }
        }

        if count < page_size || all_events.len() >= limit * 2 {
            break;
        }
        offset += page_size;

        if offset > 1000 {
            break;
        }
    }

    // Sort by liquidity/volume
    all_events.sort_by(|a, b| {
        let liq_a: f64 = a.liquidity.as_ref()
            .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0.0);
        let liq_b: f64 = b.liquidity.as_ref()
            .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0.0);
        liq_b.partial_cmp(&liq_a).unwrap_or(std::cmp::Ordering::Equal)
    });

    // Take top N
    all_events.truncate(limit);

    println!("   Found {} neg-risk markets in {:?}", all_events.len(), start.elapsed());

    Ok(all_events)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║           MARKET WATCH - Real-time Price Monitor                 ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    let client = reqwest::Client::new();
    let mut token_info: HashMap<String, TokenInfo> = HashMap::new();
    let mut watch_token_ids: Vec<String> = Vec::new();

    // Mode 1: Single token
    if let Some(token_id) = &args.token {
        println!("Mode: Single token");
        println!("   Token: {}...{}", &token_id[..20.min(token_id.len())],
                 &token_id[token_id.len().saturating_sub(10)..]);

        watch_token_ids.push(token_id.clone());
        token_info.insert(token_id.clone(), TokenInfo {
            market_title: "Custom".to_string(),
            outcome_name: args.token_name.clone(),
            token_type: "?".to_string(),
            market_idx: 0,
        });
    }
    // Mode 2: Single market by slug
    else if let Some(slug) = &args.slug {
        println!("Mode: Single market");
        let resp = client
            .get(format!("{}/events", GAMMA_API))
            .query(&[("slug", slug)])
            .send()
            .await?;

        let events: Vec<GammaEvent> = resp.json().await?;
        let event = events.first()
            .ok_or_else(|| anyhow::anyhow!("Market not found: {}", slug))?;

        let title = event.title.as_deref().unwrap_or("?");
        println!("   Market: {}", title);

        if let Some(markets) = &event.markets {
            for mkt in markets {
                if mkt.closed.unwrap_or(false) || !mkt.accepting_orders.unwrap_or(true) {
                    continue;
                }

                let clob_tokens = match &mkt.clob_token_ids {
                    Some(serde_json::Value::Array(arr)) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>(),
                    Some(serde_json::Value::String(s)) => {
                        serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
                    }
                    _ => continue,
                };

                if clob_tokens.len() < 2 {
                    continue;
                }

                let question = mkt.question.as_deref().unwrap_or("?").to_string();

                // YES token
                token_info.insert(clob_tokens[0].clone(), TokenInfo {
                    market_title: title.to_string(),
                    outcome_name: question.clone(),
                    token_type: "YES".to_string(),
                    market_idx: 0,
                });
                watch_token_ids.push(clob_tokens[0].clone());

                // NO token
                token_info.insert(clob_tokens[1].clone(), TokenInfo {
                    market_title: title.to_string(),
                    outcome_name: question.clone(),
                    token_type: "NO".to_string(),
                    market_idx: 0,
                });
                watch_token_ids.push(clob_tokens[1].clone());
            }
        }
    }
    // Mode 3: Top N neg-risk markets
    else {
        let num_markets = args.top_markets.unwrap_or(100);
        println!("Mode: Top {} liquid neg-risk markets", num_markets);

        let events = fetch_top_neg_risk_markets(&client, num_markets).await?;

        println!("\n② Processing markets and extracting tokens...");

        for (market_idx, event) in events.iter().enumerate() {
            let title = event.title.as_deref().unwrap_or("?");
            let liquidity: f64 = event.liquidity.as_ref()
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0.0);

            if let Some(markets) = &event.markets {
                let mut outcome_count = 0;
                for mkt in markets {
                    if mkt.closed.unwrap_or(false) || !mkt.accepting_orders.unwrap_or(true) {
                        continue;
                    }

                    let clob_tokens = match &mkt.clob_token_ids {
                        Some(serde_json::Value::Array(arr)) => arr
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>(),
                        Some(serde_json::Value::String(s)) => {
                            serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
                        }
                        _ => continue,
                    };

                    if clob_tokens.len() < 2 {
                        continue;
                    }

                    let question = mkt.question.as_deref().unwrap_or("?").to_string();

                    // YES token
                    token_info.insert(clob_tokens[0].clone(), TokenInfo {
                        market_title: title.to_string(),
                        outcome_name: question.clone(),
                        token_type: "YES".to_string(),
                        market_idx,
                    });
                    watch_token_ids.push(clob_tokens[0].clone());

                    // NO token
                    token_info.insert(clob_tokens[1].clone(), TokenInfo {
                        market_title: title.to_string(),
                        outcome_name: question.clone(),
                        token_type: "NO".to_string(),
                        market_idx,
                    });
                    watch_token_ids.push(clob_tokens[1].clone());

                    outcome_count += 1;
                }

                if market_idx < 20 || outcome_count > 10 {
                    println!("   #{:2} [{:3} outcomes] ${:.0}k liq - {}",
                             market_idx + 1, outcome_count, liquidity / 1000.0,
                             &title[..title.len().min(50)]);
                }
            }
        }

        if events.len() > 20 {
            println!("   ... and {} more markets", events.len() - 20);
        }
    }

    if watch_token_ids.is_empty() {
        return Err(anyhow::anyhow!("No tokens to monitor"));
    }

    println!("\n   Total tokens to monitor: {} ({} YES + {} NO)",
             watch_token_ids.len(),
             watch_token_ids.len() / 2,
             watch_token_ids.len() / 2);

    // Connect to WebSocket
    println!("\n③ Connecting to WebSocket...");
    let ws_start = Instant::now();

    let (mut ws_stream, _) = connect_async(WS_URL).await?;
    println!("   Connected in {:?}", ws_start.elapsed());

    // Subscribe in batches (Polymarket may have limits)
    println!("\n④ Subscribing to {} tokens...", watch_token_ids.len());

    let batch_size = 500;
    for (i, chunk) in watch_token_ids.chunks(batch_size).enumerate() {
        let sub_msg = SubscribeMsg {
            msg_type: "subscribe".to_string(),
            channel: "market".to_string(),
            assets_ids: chunk.to_vec(),
        };

        ws_stream
            .send(Message::Text(serde_json::to_string(&sub_msg)?.into()))
            .await?;

        println!("   Batch {}: {} tokens", i + 1, chunk.len());

        if i < watch_token_ids.len() / batch_size {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Monitor
    println!("\n⑤ Monitoring (Ctrl+C to stop)...");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("{:12} {:4} {:>7} {:>7}  {:30} {}",
             "TIME", "TYPE", "ASK", "BID", "OUTCOME", "MARKET");
    println!("═══════════════════════════════════════════════════════════════════════════════");

    let mut price_states: HashMap<String, PriceState> = HashMap::new();
    let mut message_count: u64 = 0;
    let mut price_change_count: u64 = 0;
    let monitor_start = Instant::now();

    let duration = if args.duration > 0 {
        Some(Duration::from_secs(args.duration))
    } else {
        None
    };

    loop {
        // Check duration
        if let Some(d) = duration {
            if monitor_start.elapsed() > d {
                println!("\nDuration limit reached");
                break;
            }
        }

        let msg = tokio::time::timeout(Duration::from_secs(30), ws_stream.next()).await;

        match msg {
            Ok(Some(Ok(Message::Text(text)))) => {
                let recv_time = Instant::now();
                message_count += 1;

                if args.raw && message_count <= 10 {
                    // Keep enough bytes to see the full `price_changes` payload (incl. side/size),
                    // without dumping huge book snapshots.
                    let preview_len = 1200;
                    let preview = if text.len() > preview_len {
                        &text[..preview_len]
                    } else {
                        &text
                    };
                    println!("[RAW] {}", preview);
                }

                // Handle array messages (book snapshots) - MOST ACCURATE!
                if text.starts_with('[') {
                    let book_events: Vec<BookEvent> = match serde_json::from_str(&text) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    for event in book_events {
                        if event.event_type.as_deref() != Some("book") {
                            continue;
                        }

                        let Some(asset_id) = &event.asset_id else {
                            continue;
                        };

                        let state = price_states.entry(asset_id.clone()).or_default();
                        let old_ask = state.best_ask;
                        let old_bid = state.best_bid;

                        // Get REAL best ask (lowest ask with size > 0)
                        state.best_ask = event.asks.as_ref()
                            .and_then(|asks| {
                                asks.iter()
                                    .filter_map(|l| {
                                        let price = Decimal::from_str(l.price.as_ref()?).ok()?;
                                        let size = Decimal::from_str(l.size.as_ref()?).ok()?;
                                        if size > Decimal::ZERO { Some(price) } else { None }
                                    })
                                    .min()
                            });

                        // Get REAL best bid (highest bid with size > 0)
                        state.best_bid = event.bids.as_ref()
                            .and_then(|bids| {
                                bids.iter()
                                    .filter_map(|l| {
                                        let price = Decimal::from_str(l.price.as_ref()?).ok()?;
                                        let size = Decimal::from_str(l.size.as_ref()?).ok()?;
                                        if size > Decimal::ZERO { Some(price) } else { None }
                                    })
                                    .max()
                            });

                        state.last_update = recv_time;
                        state.update_count += 1;

                        let ask_changed = state.best_ask != old_ask;
                        let bid_changed = state.best_bid != old_bid;

                        if ask_changed || bid_changed {
                            if let Some(info) = token_info.get(asset_id) {
                                price_change_count += 1;

                                let outcome_short = if info.outcome_name.len() > 25 {
                                    format!("{}...", &info.outcome_name[..22])
                                } else {
                                    info.outcome_name.clone()
                                };

                                let market_tag = format!("[M{:02}]", info.market_idx + 1);

                                println!("▶ {:12} {:4} {:>7} {:>7}  {:25} {} {}",
                                         now_str(),
                                         info.token_type,
                                         format_price(state.best_ask),
                                         format_price(state.best_bid),
                                         outcome_short,
                                         market_tag,
                                         &info.market_title[..info.market_title.len().min(35)]);
                            }
                        }
                    }
                    continue;
                }

                // Handle dict messages (price_changes) - less accurate but faster
                let ws_msg: WsMessage = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if let Some(changes) = &ws_msg.price_changes {
                    for change in changes {
                        let Some(asset_id) = &change.asset_id else {
                            continue;
                        };

                        let state = price_states.entry(asset_id.clone()).or_default();
                        let old_ask = state.best_ask;
                        let old_bid = state.best_bid;

                        if let Some(ask_str) = &change.best_ask {
                            state.best_ask = Decimal::from_str(ask_str).ok();
                        }
                        if let Some(bid_str) = &change.best_bid {
                            state.best_bid = Decimal::from_str(bid_str).ok();
                        }

                        state.last_update = recv_time;
                        state.update_count += 1;

                        let ask_changed = state.best_ask != old_ask;
                        let bid_changed = state.best_bid != old_bid;

                        if ask_changed || bid_changed || args.verbose {
                            if let Some(info) = token_info.get(asset_id) {
                                let marker = if ask_changed || bid_changed { "▶" } else { " " };

                                if ask_changed || bid_changed {
                                    price_change_count += 1;
                                }

                                let outcome_short = if info.outcome_name.len() > 25 {
                                    format!("{}...", &info.outcome_name[..22])
                                } else {
                                    info.outcome_name.clone()
                                };

                                // Show market index for disambiguation
                                let market_tag = format!("[M{:02}]", info.market_idx + 1);

                                println!("{} {:12} {:4} {:>7} {:>7}  {:25} {} {}",
                                         marker,
                                         now_str(),
                                         info.token_type,
                                         format_price(state.best_ask),
                                         format_price(state.best_bid),
                                         outcome_short,
                                         market_tag,
                                         &info.market_title[..info.market_title.len().min(35)]);
                            }
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) => {
                let _ = ws_stream.send(Message::Pong(vec![])).await;
            }
            Ok(Some(Ok(Message::Close(_)))) => {
                println!("WebSocket closed");
                break;
            }
            Ok(None) => {
                println!("WebSocket ended");
                break;
            }
            Ok(Some(Err(e))) => {
                println!("WebSocket error: {}", e);
                break;
            }
            Err(_) => {
                // Timeout - heartbeat
                println!("  {:12} [HEARTBEAT] msgs={}, changes={}, tokens_active={}, uptime={:.0}s",
                         now_str(),
                         message_count,
                         price_change_count,
                         price_states.len(),
                         monitor_start.elapsed().as_secs_f64());
            }
            _ => {} // Ignore other message types
        }
    }

    // Summary
    println!("\n═══════════════════════════════════════════════════════════════════════════════");
    println!("                              SUMMARY");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("  Messages received:    {}", message_count);
    println!("  Price changes:        {}", price_change_count);
    println!("  Tokens with updates:  {}", price_states.len());
    println!("  Duration:             {:.1}s", monitor_start.elapsed().as_secs_f64());
    println!("  Msg/sec:              {:.1}", message_count as f64 / monitor_start.elapsed().as_secs_f64());

    // Show final prices by market
    if !price_states.is_empty() && price_states.len() < 200 {
        println!("\n  Final prices:");

        let mut by_market: HashMap<String, Vec<(&String, &PriceState, &TokenInfo)>> = HashMap::new();
        for (token_id, state) in &price_states {
            if let Some(info) = token_info.get(token_id) {
                by_market
                    .entry(info.market_title.clone())
                    .or_default()
                    .push((token_id, state, info));
            }
        }

        for (market, tokens) in by_market.iter().take(10) {
            println!("\n    {}", market);
            for (_, state, info) in tokens.iter().take(10) {
                println!("      {} {:4}: ask={} bid={}",
                         &info.outcome_name[..info.outcome_name.len().min(25)],
                         info.token_type,
                         format_price(state.best_ask),
                         format_price(state.best_bid));
            }
        }
    }

    Ok(())
}
