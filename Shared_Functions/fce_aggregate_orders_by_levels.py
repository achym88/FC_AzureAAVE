def aggregate_orders_by_levels(orders, current_price, is_asks=True):
    """
    Agreguje objednávky podle dynamických cenových úrovní (pro asks i bids).

    Args:
        orders (list): Seznam objednávek ve formátu [[price, quantity], ...]
        current_price (float): Aktuální cena (procenta se počítají vůči ní)
        is_asks (bool): True pro asks, False pro bids

    Returns:
        list: Seznam agregovaných úrovní [(price, quantity_usd, level_label), ...]
    """
    if is_asks:
        level_ranges = [
            {"min": 0, "max": 0.5, "label": "0-0.5%"},
            {"min": 0.5, "max": 1.5, "label": "0.5-1.5%"},
            {"min": 1.5, "max": 3, "label": "1.5-3%"}
        ]
    else:
        level_ranges = [
            {"min": -0.5, "max": 0, "label": "0 to -0.5%"},
            {"min": -1.5, "max": -0.5, "label": "-0.5 to -1.5%"},
            {"min": -3, "max": -1.5, "label": "-1.5 to -3%"}
        ]

    aggregated = {level["label"]: [] for level in level_ranges}

    for price, quantity in orders:
        price_float = float(price)
        quantity_float = float(quantity)
        quantity_usd = quantity_float * price_float
        price_diff_percent = ((price_float - current_price) / current_price) * 100

        for level in level_ranges:
            if is_asks:
                if level["min"] <= price_diff_percent <= level["max"]:
                    aggregated[level["label"]].append((price_float, quantity_usd))
                    break
            else:
                if level["min"] <= price_diff_percent < level["max"]:
                    aggregated[level["label"]].append((price_float, quantity_usd))
                    break

    result = []
    for level in level_ranges:
        orders_in_level = aggregated[level["label"]]
        if orders_in_level:
            if is_asks:
                min_price = min(order[0] for order in orders_in_level)
                total_quantity_usd = sum(order[1] for order in orders_in_level)
                result.append((min_price, total_quantity_usd, level["label"]))
            else:
                max_price = max(order[0] for order in orders_in_level)
                total_quantity_usd = sum(order[1] for order in orders_in_level)
                result.append((max_price, total_quantity_usd, level["label"]))

    # Seřazení: asks vzestupně, bids sestupně
    return sorted(result, key=lambda x: x[0], reverse=not is_asks)