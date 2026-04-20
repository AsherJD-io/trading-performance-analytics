import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

# -------------------------
# LOAD DATA
# -------------------------
daily = pd.read_csv("data/processed/daily_performance_enriched.csv")
trades = pd.read_csv("data/processed/canonical_trades.csv")

daily["date"] = pd.to_datetime(daily["date"])
trades["close_time"] = pd.to_datetime(trades["close_time"])

# -------------------------
# METRICS
# -------------------------
total_pnl = trades["pnl"].sum()
win_rate = (trades["pnl"] > 0).mean() * 100
avg_win = trades[trades["pnl"] > 0]["pnl"].mean()
avg_loss = trades[trades["pnl"] < 0]["pnl"].mean()

# drawdown
trades = trades.sort_values("close_time")
trades["cum_pnl"] = trades["pnl"].cumsum()
rolling_max = trades["cum_pnl"].cummax()
drawdown = trades["cum_pnl"] - rolling_max
max_dd = drawdown.min()

# -------------------------
# HEADER
# -------------------------
st.title("Trading Performance Dashboard")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total PnL", f"{total_pnl:.2f}")
col2.metric("Win Rate", f"{win_rate:.2f}%")
col3.metric("Avg Win", f"{avg_win:.2f}")
col4.metric("Avg Loss", f"{avg_loss:.2f}")

st.metric("Max Drawdown", f"{max_dd:.2f}")

# -------------------------
# EQUITY CURVE (SMOOTHED)
# -------------------------
st.subheader("Equity Curve")

daily["smooth_equity"] = daily["cumulative_pnl"].rolling(5).mean()

st.line_chart(
    daily.set_index("date")[["cumulative_pnl", "smooth_equity"]]
)

# -------------------------
# DRAWDOWN
# -------------------------
st.subheader("Drawdown")

dd_df = pd.DataFrame({
    "date": trades["close_time"],
    "drawdown": drawdown
}).set_index("date")

st.line_chart(dd_df)

# -------------------------
# OVERTRADING ANALYSIS
# -------------------------
st.subheader("Overtrading Impact")

summary = daily.groupby("overtrading")["pnl"].mean()

st.write("Average PnL:")
st.bar_chart(summary)

# -------------------------
# STOP-OUT DISTRIBUTION
# -------------------------
st.subheader("Stop-out Activity")

stopouts = trades[trades["sl"] == trades["close_price"]]

if not stopouts.empty:
    st.bar_chart(stopouts["symbol"].value_counts())
else:
    st.write("No stop-outs detected")

