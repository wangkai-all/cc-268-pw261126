"""Pipeline definition for the demo scenario.

Stages (a small fixed vocabulary, not SQL):
  valid_orders   filter status == VALID
  dedup_orders   dedupe business duplicates (O3/O9 identical)
  joined         join orders -> customers (one-to-many fan-out preserved)
  region_summary group by region: sum/count/count_distinct
  channel_summary group by channel: multi-way aggregate over the same source
"""

PIPELINE = {
    "name": "订单-客户区域汇总（带行级证据）",
    "stages": [
        {
            "stage": "valid_orders",
            "type": "filter",
            "input": "orders",
            "column": "status",
            "op": "==",
            "value": "VALID",
        },
        {
            "stage": "dedup_orders",
            "type": "dedupe",
            "input": "valid_orders",
            "keys": ["customer_id", "amount", "channel"],
        },
        {
            "stage": "joined",
            "type": "join",
            "left": "dedup_orders",
            "right": "customers",
            "left_on": "customer_id",
            "right_on": "customer_id",
            "how": "inner",
        },
        {
            "stage": "region_summary",
            "type": "group",
            "input": "joined",
            "keys": ["region"],
            "aggregates": [
                {"alias": "total_amount", "func": "sum", "column": "amount"},
                {"alias": "order_count", "func": "count"},
                {"alias": "distinct_customers", "func": "count_distinct", "column": "customer_id"},
            ],
        },
        {
            "stage": "channel_summary",
            "type": "group",
            "input": "joined",
            "keys": ["channel"],
            "aggregates": [
                {"alias": "total_amount", "func": "sum", "column": "amount"},
                {"alias": "order_count", "func": "count"},
            ],
        },
    ],
}

SOURCE_FILES = {
    "orders": "examples/data/orders.csv",
    "customers": "examples/data/customers.csv",
}
