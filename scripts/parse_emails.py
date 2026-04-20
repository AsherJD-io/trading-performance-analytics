import os
import re
import pandas as pd
from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup

INPUT_DIR = "data/raw/email_exports"
OUTPUT_FILE = "data/processed/canonical_trades.csv"

records = []

pattern = re.compile(
    r"(\d+)\s+"
    r"(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(buy|sell)\s+"
    r"([\d\.]+)\s+"
    r"([a-zA-Z0-9]+)\s+"
    r"([\d\.]+)\s+"
    r"([\d\.]+|0\.00)\s+"
    r"([\d\.]+|0\.00)\s+"
    r"(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"([\d\.]+)\s+"
    r"([-\d\.]+)\s+"
    r"([-\d\.]+)\s+"
    r"([-\d\.]+)"
)

def extract_body(msg):
    # try plain text first
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                return part.get_content()

        # fallback to html
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html = part.get_content()
                soup = BeautifulSoup(html, "html.parser")
                return soup.get_text(separator=" ")
    else:
        if msg.get_content_type() == "text/html":
            soup = BeautifulSoup(msg.get_content(), "html.parser")
            return soup.get_text(separator=" ")
        return msg.get_content()

    return ""


def extract_trades(text):
    trades = []
    for match in pattern.finditer(text):
        trade = {
            "ticket": match.group(1),
            "open_time": match.group(2),
            "type": match.group(3),
            "size": match.group(4),
            "symbol": match.group(5),
            "open_price": match.group(6),
            "sl": match.group(7),
            "tp": match.group(8),
            "close_time": match.group(9),
            "close_price": match.group(10),
            "commission": match.group(11),
            "swap": match.group(12),
            "pnl": match.group(13),
        }
        trades.append(trade)
    return trades


for file in os.listdir(INPUT_DIR):
    if not file.endswith(".eml"):
        continue

    path = os.path.join(INPUT_DIR, file)

    with open(path, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    body = extract_body(msg)

    trades = extract_trades(body)
    records.extend(trades)

df = pd.DataFrame(records)

if df.empty:
    print("❌ Still no trades parsed — format may differ slightly")
    exit()

df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")

numeric_cols = ["size", "open_price", "sl", "tp", "close_price", "commission", "swap", "pnl"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.dropna(subset=["ticket", "close_time"])
df = df.sort_values("close_time").reset_index(drop=True)

os.makedirs("data/processed", exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False)

print("✅ Parsed trades:", len(df))
print("✅ Saved to:", OUTPUT_FILE)
