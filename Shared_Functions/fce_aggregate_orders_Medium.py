def aggregate_orders_by_levels_medium(orders, current_price, is_asks=True):
    # Předpočítat level ranges pouze jednou při inicializaci
    level_ranges = ([
        {"min": 0, "max": 0.25, "label": "0-0.25%"},
        {"min": 0.25, "max": 1, "label": "0.25-1%"}
    ] if is_asks else [
        {"min": -0.25, "max": 0, "label": "0 to -0.25%"},
        {"min": -1, "max": -0.25, "label": "-0.25 to -1%"}
    ])

    # Předalokace slovníku
    aggregated = {level["label"]: [] for level in level_ranges}

    # Předpočítat konstanty
    current_price_float = float(current_price)

    for price, quantity in orders:
        price_float = float(price)
        quantity_usd = float(quantity) * price_float
        price_diff_percent = ((price_float - current_price_float) / current_price_float) * 100

        if is_asks:
            if 0 <= price_diff_percent <= 0.25:
                aggregated[level_ranges[0]["label"]].append((price_float, quantity_usd))
            elif 0.25 < price_diff_percent <= 1:
                aggregated[level_ranges[1]["label"]].append((price_float, quantity_usd))
        else:
            if -0.25 <= price_diff_percent < 0:
                aggregated[level_ranges[0]["label"]].append((price_float, quantity_usd))
            elif -1 <= price_diff_percent < -0.25:
                aggregated[level_ranges[1]["label"]].append((price_float, quantity_usd))

    result = []
    price_func = min if is_asks else max

    for level in level_ranges:
        orders_in_level = aggregated[level["label"]]
        if orders_in_level:
            best_price = price_func(order[0] for order in orders_in_level)
            total_quantity_usd = sum(order[1] for order in orders_in_level)
            result.append((best_price, total_quantity_usd, level["label"]))

    return sorted(result, key=lambda x: x[0], reverse=not is_asks)