import pandas as pd
import os

df = pd.read_csv("data/processed/canonical_trades_combined.csv")

df["close_time"] = pd.to_datetime(df["close_time"])
df["date"] = df["close_time"].dt.date

# -------------------------
# DAILY METRICS
# -------------------------
daily = df.groupby("date").agg(
    pnl=("pnl", "sum"),
    trade_count=("pnl", "count"),
    win_trades=("pnl", lambda x: (x > 0).sum()),
    loss_trades=("pnl", lambda x: (x < 0).sum())
).reset_index()

daily["cumulative_pnl"] = daily["pnl"].cumsum()

# -------------------------
# OVERTRADING FLAG
# -------------------------
daily["overtrading"] = daily["trade_count"] > 40

# -------------------------
# SAVE
# -------------------------
os.makedirs("data/processed", exist_ok=True)

daily.to_csv("data/processed/daily_performance_enriched.csv", index=False)

print("✅ Daily analytics:", daily.shape)
print("Total PnL:", daily["pnl"].sum())
