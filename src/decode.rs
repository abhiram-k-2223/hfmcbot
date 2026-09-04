//! On-chain decoders for the M1 live feed (spec §3).
//!
//! Pure functions over instruction bytes — no I/O, no clock, no network — so
//! every decoder is unit-testable against captured mainnet fixtures
//! (`tests/fixtures/pump_*.json`: real `buy` / `buy_v2` / `sell` transactions
//! fetched Sep 2026; `pump_curve.json`: a real live bonding-curve account).
//! Layout authority is the official Pump.fun IDL (`pump-fun/pump-public-docs`,
//! v0.1.0), cross-checked byte-for-byte against those fixtures.
//!
//! Two deliberate forward-compat rules, both learned the hard way:
//! - instruction data may carry TRAILING bytes beyond the IDL (live `buy_v2`
//!   fixtures show +9 appended accounts / extended args for cashback flows) —
//!   parsers require the prefix they understand and ignore the tail;
//! - the live bonding-curve account is 151 bytes vs the 115 the IDL fields
//!   sum to (newer fields appended) — [`parse_curve_account`] requires the
//!   Anchor discriminator + the 49-byte prefix and ignores the tail.
//!
//! Anything with an unknown discriminator returns `None`; the feed counts
//! those as `unknown_ix` (early warning for the next program upgrade — v1 to
//! v2 already happened once, most live launches are now `create_v2`).

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

// ---------------------------------------------------------------------------
// Programs (addresses verified: pump program is the IDL authority; the AMM
// address below was confirmed `executable:true` via `getAccountInfo` — a
// second address circulating in blog posts returns `null` and is a typo).
// ---------------------------------------------------------------------------

/// Pump.fun bonding-curve program.
pub const PUMP_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
/// PumpSwap AMM program (post-graduation pools).
pub const PUMPSWAP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

/// Anchor `account:BondingCurve` discriminator (sha256 prefix, 8 bytes).
pub const CURVE_ACCOUNT_DISC: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];

/// Instruction discriminators (sha256("global:<name>") prefix, per IDL).
pub const CREATE_DISC: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
pub const CREATE_V2_DISC: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];
pub const BUY_DISC: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
pub const SELL_DISC: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
pub const BUY_V2_DISC: [u8; 8] = [184, 23, 238, 97, 103, 197, 211, 61];
pub const SELL_V2_DISC: [u8; 8] = [93, 246, 130, 60, 231, 233, 64, 178];
pub const MIGRATE_DISC: [u8; 8] = [155, 234, 231, 146, 236, 158, 162, 30];

/// Pump.fun token base-unit decimals (all curve mints are 6dp).
pub const TOKEN_BASE_DECIMALS: u32 = 6;

// ---------------------------------------------------------------------------
// Borsh primitives
// ---------------------------------------------------------------------------

fn read_u64(data: &[u8], off: &mut usize) -> Option<u64> {
    let end = off.checked_add(8)?;
    let bytes: [u8; 8] = data.get(*off..end)?.try_into().ok()?;
    *off = end;
    Some(u64::from_le_bytes(bytes))
}

fn read_borsh_str(data: &[u8], off: &mut usize) -> Option<String> {
    let end = off.checked_add(4)?;
    let len = u32::from_le_bytes(data.get(*off..end)?.try_into().ok()?) as usize;
    *off = end;
    let end = off.checked_add(len)?;
    let s = std::str::from_utf8(data.get(*off..end)?).ok()?.to_string();
    *off = end;
    Some(s)
}

fn read_pubkey(data: &[u8], off: &mut usize) -> Option<[u8; 32]> {
    let end = off.checked_add(32)?;
    let key: [u8; 32] = data.get(*off..end)?.try_into().ok()?;
    *off = end;
    Some(key)
}

// ---------------------------------------------------------------------------
// create / create_v2
// ---------------------------------------------------------------------------

/// A decoded token-creation instruction (v1 or v2 — same account head and
/// same leading args per the IDL: `mint = accounts[0]`,
/// `bonding_curve = accounts[2]`, `creator` from args).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIx {
    pub v2: bool,
    pub mint: String,
    pub bonding_curve: String,
    pub creator: [u8; 32],
    pub name: String,
    pub symbol: String,
    pub uri: String,
    /// v2 mayhem mode flag (false for v1 / when absent).
    pub mayhem: bool,
}

/// Decode a Pump.fun `create`/`create_v2` instruction. `accounts` are base58
/// account keys in instruction order. Returns `None` for wrong program,
/// unknown discriminator, short account lists, or malformed args.
pub fn parse_create(program: &str, accounts: &[String], data: &[u8]) -> Option<CreateIx> {
    if program != PUMP_PROGRAM || data.len() < 8 || accounts.len() < 3 {
        return None;
    }
    let disc: [u8; 8] = data[0..8].try_into().ok()?;
    let v2 = match disc {
        CREATE_DISC => false,
        CREATE_V2_DISC => true,
        _ => return None,
    };
    let mut off = 8;
    let name = read_borsh_str(data, &mut off)?;
    let symbol = read_borsh_str(data, &mut off)?;
    let uri = read_borsh_str(data, &mut off)?;
    let creator = read_pubkey(data, &mut off)?;
    // v2 extras (mayhem bool, cashback OptionBool): tolerated when absent —
    // only the flag we consume is read, the tail is ignored.
    let mayhem = v2 && data.get(off).is_some_and(|b| *b != 0);
    Some(CreateIx {
        v2,
        mint: accounts[0].clone(),
        bonding_curve: accounts[2].clone(),
        creator,
        name,
        symbol,
        uri,
        mayhem,
    })
}

// ---------------------------------------------------------------------------
// buy / sell / buy_v2 / sell_v2
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeKind {
    Buy,
    Sell,
}

/// A decoded bonding-curve trade. `amount_base_units` is the token quantity
/// (6dp base units); `limit_quote_units` is `max_sol_cost` (buys) or
/// `min_sol_output` (sells) in quote base units (lamports for SOL quotes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIx {
    pub kind: TradeKind,
    pub v2: bool,
    pub mint: String,
    pub bonding_curve: String,
    pub amount_base_units: u64,
    pub limit_quote_units: u64,
}

/// Decode a Pump.fun curve trade. v1: `mint = accounts[2]`,
/// `bonding_curve = accounts[3]`. v2 (multi-quote): `base_mint = accounts[1]`,
/// `bonding_curve = accounts[10]`. Unknown discriminators → `None`.
pub fn parse_trade(program: &str, accounts: &[String], data: &[u8]) -> Option<TradeIx> {
    if program != PUMP_PROGRAM || data.len() < 24 {
        return None;
    }
    let disc: [u8; 8] = data[0..8].try_into().ok()?;
    let (kind, v2) = match disc {
        BUY_DISC => (TradeKind::Buy, false),
        SELL_DISC => (TradeKind::Sell, false),
        BUY_V2_DISC => (TradeKind::Buy, true),
        SELL_V2_DISC => (TradeKind::Sell, true),
        _ => return None,
    };
    let (mint_idx, curve_idx, min_accts) = match v2 {
        false => (2, 3, 4),
        true => (1, 10, 11),
    };
    if accounts.len() < min_accts {
        return None;
    }
    let mut off = 8;
    let amount_base_units = read_u64(data, &mut off)?;
    let limit_quote_units = read_u64(data, &mut off)?;
    Some(TradeIx {
        kind,
        v2,
        mint: accounts[mint_idx].clone(),
        bonding_curve: accounts[curve_idx].clone(),
        amount_base_units,
        limit_quote_units,
    })
}

// ---------------------------------------------------------------------------
// migrate (graduation signal)
// ---------------------------------------------------------------------------

/// A decoded `migrate` instruction: the curve graduated to a PumpSwap pool.
/// Per IDL: `mint = accounts[2]`, `bonding_curve = accounts[3]`,
/// `pool = accounts[9]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateIx {
    pub mint: String,
    pub bonding_curve: String,
    pub pool: String,
}

pub fn parse_migrate(program: &str, accounts: &[String], data: &[u8]) -> Option<MigrateIx> {
    if program != PUMP_PROGRAM || data.len() < 8 || accounts.len() < 10 {
        return None;
    }
    let disc: [u8; 8] = data[0..8].try_into().ok()?;
    if disc != MIGRATE_DISC {
        return None;
    }
    Some(MigrateIx {
        mint: accounts[2].clone(),
        bonding_curve: accounts[3].clone(),
        pool: accounts[9].clone(),
    })
}

// ---------------------------------------------------------------------------
// Bonding-curve account + Decimal price math (spec §3: curve math in Decimal)
// ---------------------------------------------------------------------------

/// Live reserve snapshot of a bonding-curve account (IDL `BondingCurve` head:
/// 8-byte discriminator + 5×u64 + `complete` bool; the tail — creator,
/// mayhem/cashback flags, quote mint — is intentionally not parsed here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurveReserves {
    pub virtual_base_units: u64,
    pub virtual_quote_units: u64,
    pub real_base_units: u64,
    pub real_quote_units: u64,
    pub total_supply_units: u64,
    pub complete: bool,
}

/// Parse the 49-byte head of a bonding-curve account. Requires the Anchor
/// account discriminator; longer accounts (newer fields appended) are
/// accepted, shorter ones rejected.
pub fn parse_curve_account(data: &[u8]) -> Option<CurveReserves> {
    if data.len() < 49 {
        return None;
    }
    let disc: [u8; 8] = data[0..8].try_into().ok()?;
    if disc != CURVE_ACCOUNT_DISC {
        return None;
    }
    let mut off = 8;
    let virtual_base_units = read_u64(data, &mut off)?;
    let virtual_quote_units = read_u64(data, &mut off)?;
    let real_base_units = read_u64(data, &mut off)?;
    let real_quote_units = read_u64(data, &mut off)?;
    let total_supply_units = read_u64(data, &mut off)?;
    let complete = data.get(off).is_some_and(|b| *b != 0);
    Some(CurveReserves {
        virtual_base_units,
        virtual_quote_units,
        real_base_units,
        real_quote_units,
        total_supply_units,
        complete,
    })
}

/// Constant-product quote price: `virtual_quote / virtual_base`, adjusted for
/// token decimals (base 6dp, quote `quote_decimals`, e.g. 9 for SOL/wSOL).
/// Pure Decimal — no floats. `None` on empty reserves.
pub fn curve_price_in_quote(r: &CurveReserves, quote_decimals: u32) -> Option<Decimal> {
    if r.virtual_base_units == 0 || r.virtual_quote_units == 0 {
        return None;
    }
    let base = Decimal::from(r.virtual_base_units) / Decimal::from(10u64.pow(TOKEN_BASE_DECIMALS));
    let quote =
        Decimal::from(r.virtual_quote_units) / Decimal::from(10u64.pow(quote_decimals.min(28)));
    Some(quote / base)
}

// ---------------------------------------------------------------------------
// Stonkfun public REST poller types (no key required; 1 req/s courtesy pace).
// Only the HTTP fetch loop is a soak-step — URL + response parsing live here
// and are fully tested against a captured response.
// ---------------------------------------------------------------------------

/// Stonkfun launches endpoint.
pub const STONKFUN_LAUNCHES_URL: &str = "https://www.stonkfun.xyz/api/public/v1/launches";

/// One entry of the stonkfun launch ledger. Note these launch into Raydium
/// pools against *varying* quote mints (not just SOL) — `quote_mint` must be
/// resolved to USD at enrichment time; `pool` gives the reserve account for
/// that. `start_mcap_usd` is the venue's own estimate (cross-check only).
#[derive(Debug, Clone)]
pub struct StonkLaunch {
    pub mint: String,
    pub pool: String,
    pub name: String,
    pub symbol: String,
    pub creator: String,
    pub quote_mint: String,
    pub quote_symbol: String,
    pub start_mcap_usd: Option<Decimal>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Parse a stonkfun `/launches` response body. Unknown/extra fields ignored;
/// entries missing mint/pool/creator are skipped, not fatal.
pub fn parse_stonkfun_launches(body: &str) -> Vec<StonkLaunch> {
    let root: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let items = root
        .get("data")
        .and_then(|d| d.get("launches"))
        .and_then(|l| l.as_array())
        .cloned()
        .unwrap_or_default();
    let str_field = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
    };
    let mut out = Vec::new();
    for item in items {
        let (Some(mint), Some(pool), Some(creator)) = (
            str_field(&item, "mint"),
            str_field(&item, "pool"),
            str_field(&item, "creator"),
        ) else {
            continue;
        };
        let created_at = item
            .get("createdAt")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok());
        let Some(created_at) = created_at else {
            continue;
        };
        let quote = item.get("quote").cloned().unwrap_or_default();
        let start_mcap_usd = item
            .get("startMarketCapUsd")
            .and_then(|x| x.as_f64())
            .and_then(Decimal::from_f64);
        out.push(StonkLaunch {
            mint,
            pool,
            name: str_field(&item, "name").unwrap_or_default(),
            symbol: str_field(&item, "symbol").unwrap_or_default(),
            creator,
            quote_mint: quote
                .get("mint")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            quote_symbol: quote
                .get("symbol")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            start_mcap_usd,
            created_at,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn hex_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn borsh_str(s: &str, out: &mut Vec<u8>) {
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }

    #[test]
    fn program_ids_are_the_verified_ones() {
        assert_eq!(PUMP_PROGRAM, "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
        assert_eq!(
            PUMPSWAP_AMM_PROGRAM,
            "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
        );
    }

    #[test]
    fn parses_live_buy_fixture() {
        let f = fixture("pump_buy.json");
        let accounts: Vec<String> = serde_json::from_value(f["accounts"].clone()).unwrap();
        let data = hex_bytes(f["data_hex"].as_str().unwrap());
        let t = parse_trade(PUMP_PROGRAM, &accounts, &data).unwrap();
        assert_eq!(t.kind, TradeKind::Buy);
        assert!(!t.v2);
        assert!(t.amount_base_units > 0 && t.limit_quote_units > 0);
        // v1 layout: mint = accounts[2], curve = accounts[3].
        assert_eq!(t.mint, accounts[2]);
        assert_eq!(t.bonding_curve, accounts[3]);
    }

    #[test]
    fn parses_live_buy_v2_fixture_with_extended_accounts() {
        let f = fixture("pump_buy_v2.json");
        let accounts: Vec<String> = serde_json::from_value(f["accounts"].clone()).unwrap();
        // Live v2 carries 27 accounts (IDL lists 27 incl. appended
        // cashback/volume accounts) — the parser must not choke on extras.
        assert!(accounts.len() >= 11);
        let data = hex_bytes(f["data_hex"].as_str().unwrap());
        let t = parse_trade(PUMP_PROGRAM, &accounts, &data).unwrap();
        assert_eq!(t.kind, TradeKind::Buy);
        assert!(t.v2);
        assert_eq!(t.mint, accounts[1]);
        assert_eq!(t.bonding_curve, accounts[10]);
    }

    #[test]
    fn parses_live_sell_fixture() {
        let f = fixture("pump_sell.json");
        let accounts: Vec<String> = serde_json::from_value(f["accounts"].clone()).unwrap();
        let data = hex_bytes(f["data_hex"].as_str().unwrap());
        let t = parse_trade(PUMP_PROGRAM, &accounts, &data).unwrap();
        assert_eq!(t.kind, TradeKind::Sell);
        assert!(!t.v2);
        assert_eq!(t.mint, accounts[2]);
    }

    #[test]
    fn unknown_discriminator_and_wrong_program_rejected() {
        let accounts = vec!["a".to_string(); 16];
        let mut unknown = vec![0u8; 24];
        unknown[0..8].copy_from_slice(&[9u8; 8]);
        assert!(parse_trade(PUMP_PROGRAM, &accounts, &unknown).is_none());
        assert!(parse_create(PUMP_PROGRAM, &accounts, &unknown).is_none());
        let mut buy = BUY_DISC.to_vec();
        buy.extend_from_slice(&[0u8; 16]);
        assert!(parse_trade(
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            &accounts,
            &buy
        )
        .is_none());
        // Short data / short accounts also rejected, never panics.
        assert!(parse_trade(PUMP_PROGRAM, &accounts, &buy[..10]).is_none());
        assert!(parse_trade(PUMP_PROGRAM, &accounts[..2], &buy).is_none());
    }

    #[test]
    fn create_v2_round_trip_per_idl_layout() {
        // Synthetic until the soak captures a live create: layout follows the
        // official IDL exactly (disc + name/symbol/uri + creator + mayhem).
        let mut data = CREATE_V2_DISC.to_vec();
        borsh_str("Test Token", &mut data);
        borsh_str("TEST", &mut data);
        borsh_str("https://example.com/meta.json", &mut data);
        data.extend_from_slice(&[7u8; 32]);
        data.push(1); // mayhem = true
        let accounts: Vec<String> = (0..16).map(|i| format!("acct{i}")).collect();
        let c = parse_create(PUMP_PROGRAM, &accounts, &data).unwrap();
        assert!(c.v2);
        assert_eq!(c.mint, "acct0");
        assert_eq!(c.bonding_curve, "acct2");
        assert_eq!(c.creator, [7u8; 32]);
        assert_eq!(c.name, "Test Token");
        assert_eq!(c.symbol, "TEST");
        assert!(c.mayhem);
    }

    #[test]
    fn parses_live_curve_account() {
        let f = fixture("pump_curve.json");
        assert_eq!(f["owner"].as_str().unwrap(), PUMP_PROGRAM);
        let raw = hex_bytes(f["data_hex"].as_str().unwrap());
        assert_eq!(raw.len(), 151);
        let r = parse_curve_account(&raw).unwrap();
        // Real mainnet values (Sep 2026): ~1.07B virtual tokens, ~30 SOL
        // virtual quote, pool still on-curve.
        assert_eq!(r.virtual_base_units, 1071729972785096);
        assert_eq!(r.virtual_quote_units, 30035551052);
        assert_eq!(r.total_supply_units, 1000000000000000);
        assert!(!r.complete);
        let px = curve_price_in_quote(&r, 9).unwrap();
        assert!(px > dec!(0) && px < dec!(0.000001));
        // Empty reserves → None, never div-by-zero.
        let empty = CurveReserves {
            virtual_base_units: 0,
            virtual_quote_units: 0,
            real_base_units: 0,
            real_quote_units: 0,
            total_supply_units: 0,
            complete: false,
        };
        assert!(curve_price_in_quote(&empty, 9).is_none());
    }

    #[test]
    fn curve_price_math_matches_hand_computation() {
        // 30 SOL virtual quote (9dp) over 1.073B virtual base (6dp):
        // 30 / 1_073_000_000 ≈ 2.7959e-8 SOL per token.
        let r = CurveReserves {
            virtual_base_units: 1_073_000_000_000_000,
            virtual_quote_units: 30_000_000_000,
            real_base_units: 0,
            real_quote_units: 0,
            total_supply_units: 1_000_000_000_000_000,
            complete: false,
        };
        let px = curve_price_in_quote(&r, 9).unwrap();
        let expected = dec!(30) / dec!(1073000000);
        assert!((px - expected).abs() < dec!(0.000000000001));
    }

    #[test]
    fn parses_stonkfun_launches_response() {
        let path = format!(
            "{}/tests/fixtures/stonkfun_launches.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let body = std::fs::read_to_string(path).unwrap();
        let launches = parse_stonkfun_launches(&body);
        assert_eq!(launches.len(), 2);
        let a = &launches[0];
        assert_eq!(a.symbol, "ASTRA");
        assert_eq!(a.creator, "EApkaRBSf2tCbVRtznQhwBPze9yCLhZxkjXfa397XzBe");
        assert!(!a.pool.is_empty() && !a.quote_mint.is_empty());
        assert!(a.start_mcap_usd.unwrap() > dec!(5000));
        // Garbage in → empty out, never panics.
        assert!(parse_stonkfun_launches("not json").is_empty());
        assert!(parse_stonkfun_launches(r#"{"data":{}}"#).is_empty());
    }
}
