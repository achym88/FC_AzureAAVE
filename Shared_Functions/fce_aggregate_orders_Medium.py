def aggregate_orders_by_levels_medium(orders, current_price, is_asks=True):
    # Předpočítat level ranges pouze jednou při inicializaci
    level_ranges = ([
        {"min": 0, "max": 0.25, "label": "0-0.25%"},
        {"min": 0.25, "max": 0.6, "label": "0.25-0.6%"},
        {"min": 0.6, "max": 1.5, "label": "0.6-1.5%"}
    ] if is_asks else [
        {"min": -0.25, "max": 0, "label": "0 to -0.25%"},
        {"min": -0.6, "max": -0.25, "label": "-0.25 to -0.6%"},
        {"min": -1.5, "max": -0.6, "label": "-0.6 to -1.5%"}
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
            elif 0.25 < price_diff_percent <= 0.6:
                aggregated[level_ranges[1]["label"]].append((price_float, quantity_usd))
            elif 0.6 < price_diff_percent <= 1.5:
                aggregated[level_ranges[2]["label"]].append((price_float, quantity_usd))
        else:
            if -0.25 <= price_diff_percent < 0:
                aggregated[level_ranges[0]["label"]].append((price_float, quantity_usd))
            elif -0.6 <= price_diff_percent < -0.25:
                aggregated[level_ranges[1]["label"]].append((price_float, quantity_usd))
            elif -1.5 <= price_diff_percent < -0.6:
                aggregated[level_ranges[2]["label"]].append((price_float, quantity_usd))

    result = []
    price_func = min if is_asks else max

    for level in level_ranges:
        orders_in_level = aggregated[level["label"]]
        if orders_in_level:
            best_price = price_func(order[0] for order in orders_in_level)
            total_quantity_usd = sum(order[1] for order in orders_in_level)
            result.append((best_price, total_quantity_usd, level["label"]))

    return sorted(result, key=lambda x: x[0], reverse=not is_asks)