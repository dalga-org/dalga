from dalga.core import DalgaClient, Expectation


dalga = DalgaClient(max_records=100)

rules = [
    Expectation("price").min_value(0.0),  # No negative prices
    Expectation("user_id").to_not_be_null(),  # Must have a user ID
    Expectation("category").max_null_ratio(0.50),  # Category can be null, up to 50% max
]

clean_batch = [
    {"user_id": 1, "price": 10.5, "category": "shoes"},
    {"user_id": 2, "price": 45.0, "category": None},
]

print("Validating Clean Batch.")
if dalga.validate(clean_batch, rules):
    print("Passed! Data is safe to process.")
else:
    print("Rejected!")

corrupted_batch = [
    {"user_id": 3, "price": 20.0, "category": "shirts"},
    {"user_id": 4, "price": -5.0, "category": "shoes"},  # BUG: Negative price!
]

print("Validating Corrupted Batch.")
if dalga.validate(corrupted_batch, rules):
    print("Passed!")
else:
    print("Rejected! Sending to Dead Letter Queue.")

print("Final Engine State:")
dalga._trigger_flush()
