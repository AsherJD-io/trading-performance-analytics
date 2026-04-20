import pandas as pd

INPUT_PATH = "data/processed/combined_trades_raw.csv"
OUTPUT_PATH = "data/processed/canonical_trades.csv"


def clean_symbol(symbol: str) -> str:
    if pd.isna(symbol):
        return symbol
    return symbol.replace("m", "").upper()


def map_side(side: str) -> str:
    if pd.isna(side):
        return side
    return side.lower()


def main():
    df = pd.read_csv(INPUT_PATH)

    # rename to canonical schema
    df = df.rename(columns={
        "ticket": "trade_id",
        "opening_time_utc": "opened_at",
        "closing_time_utc": "closed_at",
        "type": "side",
        "lots": "position_size",
        "opening_price": "entry_price",
        "closing_price": "exit_price",
        "profit": "pnl"
    })

    # type conversions
    df["opened_at"] = pd.to_datetime(df["opened_at"])
    df["closed_at"] = pd.to_datetime(df["closed_at"])

    # cleaning
    df["symbol"] = df["symbol"].apply(clean_symbol)
    df["side"] = df["side"].apply(map_side)

    # numeric safety
    numeric_cols = [
        "position_size",
        "entry_price",
        "exit_price",
        "stop_loss",
        "take_profit",
        "commission",
        "swap",
        "pnl"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # select final columns (explicit schema)
    final_cols = [
        "trade_id",
        "opened_at",
        "closed_at",
        "side",
        "symbol",
        "position_size",
        "entry_price",
        "exit_price",
        "stop_loss",
        "take_profit",
        "commission",
        "swap",
        "pnl",
        "close_reason",
        "broker",
        "source_type",
        "source_file"
    ]

    df = df[final_cols]

    # save
    df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Canonical dataset created")
    print("Rows:", len(df))
    print("Columns:", list(df.columns))


if __name__ == "__main__":
    main()
