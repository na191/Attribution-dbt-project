import random
import csv
from datetime import datetime, timedelta
import uuid

random.seed(42)

CHANNELS = ["paid_search", "organic_search", "email", "social_media", "direct", "referral", "paid_social"]
CAMPAIGNS = {
    "paid_search": ["spring_sale_2024", "brand_awareness_q1", "retargeting_jan"],
    "organic_search": ["seo_blog_push", "content_hub_v2", None],
    "email": ["welcome_series", "abandoned_cart", "loyalty_nudge"],
    "social_media": ["instagram_launch", "tiktok_awareness", "fb_engagement"],
    "direct": [None],
    "referral": ["partner_promo_jan", "affiliate_v2", None],
    "paid_social": ["meta_lookalike", "linkedin_b2b", "youtube_retarget"],
}
DEVICES = ["mobile", "desktop", "tablet"]
PAGES = ["/home", "/products", "/pricing", "/about", "/blog", "/signup", "/checkout"]

# --- Generate user_sessions (raw event log) ---
# Each user has 1-5 touchpoints before converting (or not)
users = []
for user_id in range(1, 2001):
    user_id_str = str(uuid.uuid4())[:12]
    num_touchpoints = random.randint(1, 6)
    converted = random.random() < 0.18  # ~18% conversion rate
    conversion_date = None
    base_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 89))  # Jan-Mar 2024
    device = random.choice(DEVICES)

    touchpoints = []
    for i in range(num_touchpoints):
        channel = random.choice(CHANNELS)
        campaign = random.choice(CAMPAIGNS[channel])
        event_date = base_date + timedelta(days=i, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        touchpoints.append({
            "session_id": str(uuid.uuid4())[:16],
            "user_id": user_id_str,
            "event_timestamp": event_date.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": channel,
            "campaign": campaign,
            "device": device,
            "page_visited": random.choice(PAGES),
            "session_duration_seconds": random.randint(15, 600),
        })

    # If converted, mark the last touchpoint's date as conversion date
    if converted:
        conversion_date = touchpoints[-1]["event_timestamp"]

    users.append({
        "user_id": user_id_str,
        "touchpoints": touchpoints,
        "converted": converted,
        "conversion_date": conversion_date,
        "order_value": round(random.uniform(29.99, 499.99), 2) if converted else None,
    })

# Write raw_events.csv
with open("/home/claude/attribution_project/raw_events.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["session_id", "user_id", "event_timestamp", "channel", "campaign", "device", "page_visited", "session_duration_seconds"])
    for user in users:
        for tp in user["touchpoints"]:
            writer.writerow([tp["session_id"], tp["user_id"], tp["event_timestamp"], tp["channel"], tp["campaign"], tp["device"], tp["page_visited"], tp["session_duration_seconds"]])

# Write conversions.csv
with open("/home/claude/attribution_project/conversions.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "converted", "conversion_timestamp", "order_value"])
    for user in users:
        writer.writerow([user["user_id"], user["converted"], user["conversion_date"] or "", user["order_value"] or ""])

# Write ad_spend.csv (daily spend per channel/campaign)
with open("/home/claude/attribution_project/ad_spend.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["spend_date", "channel", "campaign", "spend_usd", "impressions"])
    paid_channels = ["paid_search", "paid_social", "social_media", "email"]
    for day_offset in range(90):
        date = (datetime(2024, 1, 1) + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        for channel in paid_channels:
            for campaign in CAMPAIGNS[channel]:
                if campaign is None:
                    continue
                spend = round(random.uniform(50, 800), 2)
                impressions = random.randint(500, 25000)
                writer.writerow([date, channel, campaign, spend, impressions])

print("Generated: raw_events.csv, conversions.csv, ad_spend.csv")

# Print row counts
import os
for fname in ["raw_events.csv", "conversions.csv", "ad_spend.csv"]:
    path = f"/home/claude/attribution_project/{fname}"
    with open(path) as fh:
        lines = sum(1 for _ in fh) - 1
    print(f"  {fname}: {lines} rows")
