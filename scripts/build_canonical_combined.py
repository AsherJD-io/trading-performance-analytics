import pandas as pd
import glob
import os

# -------------------------
# LOAD EMAIL DATA
# -------------------------
email_df = pd.read_csv("data/processed/canonical_trades.csv")

email_df = email_df.rename(columns={
    "open_time": "open_time",
    "close_time": "close_time",
    "symbol": "symbol",
    "pnl": "pnl"
})

email_df = email_df[["open_time", "close_time", "symbol", "pnl"]]

# -------------------------
# LOAD BROKER CSVs (YOUR FORMAT)
# -------------------------
broker_files = glob.glob("data/raw/broker_csv/*.csv")

broker_dfs = []

for file in broker_files:
    try:
        df = pd.read_csv(file)

        # rename to canonical
        df = df.rename(columns={
            "opening_time_utc": "open_time",
            "closing_time_utc": "close_time",
            "symbol": "symbol",
            "profit": "pnl"
        })

        # ensure required columns exist
        required = ["open_time", "close_time", "symbol", "pnl"]
        if not all(col in df.columns for col in required):
            print(f"⚠️ Skipping {file} (missing required columns)")
            continue

        # convert types
        df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
        df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")
        df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce")

        df = df.dropna(subset=["close_time", "pnl"])

        broker_dfs.append(df[required])

    except Exception as e:
        print(f"❌ Failed {file}: {e}")

# combine broker data
if broker_dfs:
    broker_df = pd.concat(broker_dfs, ignore_index=True)
else:
    broker_df = pd.DataFrame(columns=["open_time","close_time","symbol","pnl"])

# -------------------------
# COMBINE ALL
# -------------------------
combined = pd.concat([email_df, broker_df], ignore_index=True)

# drop duplicates
combined = combined.drop_duplicates()

# -------------------------
# SAVE
# -------------------------
os.makedirs("data/processed", exist_ok=True)

combined.to_csv("data/processed/canonical_trades_combined.csv", index=False)

print("✅ Combined dataset:", combined.shape)
