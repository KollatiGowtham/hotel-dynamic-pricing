import json
import boto3
import csv
import io
import logging
from datetime import datetime, date, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET = "hotel-pricing-datalake"

BASE_PRICES = {
    "standard": 120.0,
    "deluxe": 190.0,
    "suite": 380.0
}

SEASON_MULTIPLIERS = {
    "summer": 1.30,
    "winter": 1.20,
    "spring": 1.10,
    "autumn": 1.00
}

DAY_OF_WEEK_MULTIPLIERS = {
    0: 1.00, 1: 1.00, 2: 1.02,
    3: 1.05, 4: 1.15, 5: 1.20, 6: 1.18
}

def get_season(month):
    if month in [12, 1, 2]: return "winter"
    if month in [3, 4, 5]:  return "spring"
    if month in [6, 7, 8]:  return "summer"
    return "autumn"

def read_s3_csv(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        content = obj["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)
    except Exception as e:
        logger.warning(f"Could not read {key}: {e}")
        return []

def get_occupancy_rate(hotel_id, room_type, occupancy_data):
    matches = [
        r for r in occupancy_data
        if r["hotel_id"] == hotel_id and r["room_type"] == room_type
    ]
    if not matches:
        return 0.75, 0.0
    latest = sorted(matches, key=lambda x: x["date"])[-1]
    return float(latest["occupancy_rate"]), float(latest["competitor_rate"])

def get_avg_competitor_rate(hotel_id, room_type, competitor_data):
    matches = [
        r for r in competitor_data
        if r["hotel_id"] == hotel_id and r["room_type"] == room_type
    ]
    if not matches:
        return None
    latest = sorted(matches, key=lambda x: x["date"])[-1]
    return float(latest["avg_competitor_rate"])

def calculate_advance_multiplier(advance_days):
    if advance_days <= 3:  return 0.92
    if advance_days <= 7:  return 1.00
    if advance_days <= 14: return 1.05
    if advance_days <= 30: return 1.08
    if advance_days <= 60: return 1.03
    return 0.95

def calculate_occupancy_multiplier(occupancy_rate):
    if occupancy_rate >= 0.95: return 1.35
    if occupancy_rate >= 0.85: return 1.20
    if occupancy_rate >= 0.70: return 1.08
    if occupancy_rate >= 0.50: return 1.00
    if occupancy_rate >= 0.30: return 0.92
    return 0.85

def is_holiday(check_in_date):
    HOLIDAYS = [
        date(check_in_date.year, 1, 1),
        date(check_in_date.year, 2, 14),
        date(check_in_date.year, 8, 15),
        date(check_in_date.year, 10, 2),
        date(check_in_date.year, 12, 25),
        date(check_in_date.year, 12, 31),
    ]
    for holiday in HOLIDAYS:
        if abs((check_in_date - holiday).days) <= 2:
            return True
    return False

def calculate_competitor_multiplier(our_base_price, competitor_avg):
    if competitor_avg is None: return 1.0
    ratio = our_base_price / competitor_avg
    if ratio < 0.85:  return 1.08
    if ratio < 0.95:  return 1.04
    if ratio <= 1.05: return 1.00
    if ratio <= 1.15: return 0.97
    return 0.93

def calculate_optimal_price(hotel_id, room_type, check_in_date,
                             advance_days, occupancy_data, competitor_data):
    base_price = BASE_PRICES.get(room_type, 120.0)
    season = get_season(check_in_date.month)
    dow = check_in_date.weekday()
    holiday_flag = is_holiday(check_in_date)
    occupancy_rate, _ = get_occupancy_rate(hotel_id, room_type, occupancy_data)
    competitor_avg = get_avg_competitor_rate(hotel_id, room_type, competitor_data)

    season_mult = SEASON_MULTIPLIERS.get(season, 1.0)
    dow_mult = DAY_OF_WEEK_MULTIPLIERS.get(dow, 1.0)
    advance_mult = calculate_advance_multiplier(advance_days)
    occupancy_mult = calculate_occupancy_multiplier(occupancy_rate)
    holiday_mult = 1.25 if holiday_flag else 1.0
    competitor_mult = calculate_competitor_multiplier(base_price * season_mult, competitor_avg)

    optimal_price = (
        base_price * season_mult * dow_mult *
        advance_mult * occupancy_mult *
        holiday_mult * competitor_mult
    )

    min_price = base_price * 0.60
    max_price = base_price * 1.80
    optimal_price = max(min_price, min(max_price, optimal_price))
    optimal_price = round(optimal_price, 2)

    return {
        "optimal_price": optimal_price,
        "base_price": base_price,
        "season": season,
        "season_multiplier": season_mult,
        "dow_multiplier": dow_mult,
        "advance_multiplier": advance_mult,
        "occupancy_rate": occupancy_rate,
        "occupancy_multiplier": occupancy_mult,
        "holiday_flag": holiday_flag,
        "holiday_multiplier": holiday_mult,
        "competitor_avg": competitor_avg,
        "competitor_multiplier": competitor_mult,
        "recommendation": "INCREASE" if optimal_price > base_price else "DECREASE"
    }

def save_pricing_output(results, run_timestamp):
    current_key = "pricing-output/current/prices.json"
    s3.put_object(
        Bucket=BUCKET, Key=current_key,
        Body=json.dumps(results, indent=2),
        ContentType="application/json"
    )
    history_key = f"pricing-output/history/prices_{run_timestamp}.json"
    s3.put_object(
        Bucket=BUCKET, Key=history_key,
        Body=json.dumps(results, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Saved pricing output to S3: {current_key}")

def lambda_handler(event, context):
    logger.info(f"Hotel Pricing Engine triggered at {datetime.utcnow()}")
    run_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    occupancy_data = read_s3_csv("raw/occupancy/occupancy.csv")
    competitor_data = read_s3_csv("raw/competitor-rates/competitor_rates.csv")

    hotels = ["H01", "H02"]
    room_types = ["standard", "deluxe", "suite"]
    advance_windows = [1, 7, 14, 30, 60, 90]
    today = date.today()

    results = {"generated_at": run_timestamp, "hotels": {}}

    for hotel_id in hotels:
        results["hotels"][hotel_id] = {}
        for room_type in room_types:
            results["hotels"][hotel_id][room_type] = {}
            for advance_days in advance_windows:
                check_in = today + timedelta(days=advance_days)
                pricing = calculate_optimal_price(
                    hotel_id, room_type, check_in,
                    advance_days, occupancy_data, competitor_data
                )
                results["hotels"][hotel_id][room_type][f"{advance_days}_days_advance"] = pricing

    save_pricing_output(results, run_timestamp)

    total_prices = len(hotels) * len(room_types) * len(advance_windows)
    logger.info(f"Pricing complete. {total_prices} prices calculated.")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Hotel pricing updated successfully",
            "timestamp": run_timestamp,
            "prices_calculated": total_prices,
            "output_path": f"s3://{BUCKET}/pricing-output/current/prices.json"
        })
    }
