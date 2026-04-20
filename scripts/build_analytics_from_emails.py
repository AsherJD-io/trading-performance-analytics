import pandas as pd
import os

# -------------------------
# LOAD CANONICAL DATA
# -------------------------
df = pd.read_csv("data/processed/canonical_trades.csv")

df["close_time"] = pd.to_datetime(df["close_time"])
df["date"] = df["close_time"].dt.date

# -------------------------
# DAILY AGGREGATION
# -------------------------
daily = df.groupby("date").agg(
    pnl=("pnl", "sum"),
    trade_count=("ticket", "count"),
    win_trades=("pnl", lambda x: (x > 0).sum()),
    loss_trades=("pnl", lambda x: (x < 0).sum())
).reset_index()

# -------------------------
# CUMULATIVE PNL
# -------------------------
daily = daily.sort_values("date")
daily["cumulative_pnl"] = daily["pnl"].cumsum()

# -------------------------
# OVERTRADING FLAG
# -------------------------
THRESHOLD = 40
daily["overtrading"] = daily["trade_count"] > THRESHOLD

# -------------------------
# STOP-OUT DETECTION
# -------------------------
df["close_reason"] = df.apply(
    lambda x: "so" if x["sl"] == x["close_price"] else "normal",
    axis=1
)

stopouts = df[df["close_reason"] == "so"]
stopouts_daily = stopouts.groupby("date").size()

daily["stopout_count"] = daily["date"].map(stopouts_daily).fillna(0)

# -------------------------
# SAVE OUTPUTS
# -------------------------
os.makedirs("data/processed", exist_ok=True)

daily.to_csv("data/processed/daily_performance.csv", index=False)

print("✅ Daily performance rebuilt:", daily.shape)
print("Total PnL:", daily["pnl"].sum())
