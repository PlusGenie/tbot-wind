import yaml

from ..utils.objects import OrderTVEx


def create_orders_from_yaml(yaml_file_path: str):
    """
    Create a list of OrderTVEx instances from a YAML configuration file.
    Args:
        yaml_file_path (str): Path to the YAML configuration file.

    Returns:
        List[OrderTVEx]: List of orders created from the YAML file.
    """
    with open(yaml_file_path, 'r') as file:
        config = yaml.safe_load(file)

    orders = []
    for order_config in config['orders']:
        order = OrderTVEx(
            timestamp="",  # This should be set dynamically
            contract="stock",
            symbol=order_config.get('symbol', 'GOOGL'),
            timeframe=order_config.get('timeframe', '1D'),
            timezone=order_config.get('timezone', 'US/Eastern'),
            direction=order_config.get('direction', 'strategy.entrylong'),
            qty=order_config.get('qty', 1.0),
            currency=order_config.get('currency', 'USD'),
            entryLimit=order_config.get('entryLimit', 0.0),
            entryStop=order_config.get('entryStop', 0.0),
            exitLimit=order_config.get('exitLimit', 0.0),
            exitStop=order_config.get('exitStop', 0.0),
            price=order_config.get('price', 0.0),
            orderRef=order_config.get('orderRef', 'defaultRef'),
            tif=order_config.get('tif', 'GTC'),
            exchange=order_config.get('exchange', 'SMART'),
            lastTradeDateOrContractMonth=order_config.get('lastTradeDateOrContractMonth', ''),
            multiplier=order_config.get('multiplier', '0'),
            outsideRth=order_config.get('outsideRth', False),
            duration=order_config.get('duration', '1 W'),
            bootup_duration=order_config.get('bootup_duration', '1 Y'),
            end_date=order_config.get('end_date', ''),
            indicator=order_config.get('indicator', 'macd')
        )
        orders.append(order)

    return orders


def create_order() -> OrderTVEx:
    """
    Create a customized OrderTV instance with user-defined settings.

    Args:
        symbol (str): The trading symbol.
        timeframe (str): The timeframe for trading.
            Valid values are:
            "1S": "1 secs"   # 1 second
            "5S": "5 secs"   # 5 seconds
            "10S": "10 secs" # 10 seconds
            "15S": "15 secs" # 15 seconds
            "30S": "30 secs" # 30 seconds
            "1": "1 min"     # 1 minute (no unit specified)
            "5": "5 mins"    # 5 minutes
            "15": "15 mins"  # 15 minutes
            "30": "30 mins"  # 30 minutes
            "60": "1 hour"   # 1 hour (represented as 60 minutes)
            "120": "2 hours" # 2 hours (represented as 120 minutes)
            "240": "4 hours" # 4 hours (represented as 240 minutes)
            "1D": "1 day"    # 1 day
            "1W": "1 week"   # 1 week
            "1M": "1 month"  # 1 month

        duration:
            Represents the amount of historical data you want to fetch.
            Specified as a string, e.g., "1 D" (one day), "3 M" (three months).
            The longer the duration, the more historical data you'll fetch.
            Ex: 5 D, 1 W, 2 W, 6 M, 1 Y, 2 Y

        end_date:
            Example:'20230814 15:30:00 US/Eastern'
                    '20230814 15:30:00'
                    '2023-08-14 15:30:00'
    Returns:
        OrderTV: A configured OrderTV instance.
    """
    return OrderTVEx(
        timestamp="",  # This should be set dynamically based on real-time data
        contract="stock",
        symbol="GOOGL",
        timeframe="1D",
        timezone="US/Eastern",  # Timezone for exchange, stock
        direction="",  # This should be set based on trading strategy
        qty=10.0,
        currency="USD",
        entryLimit=0.0,
        entryStop=0.0,
        exitLimit=0.0,
        exitStop=0.0,
        price=0.0,  # This should be set dynamically based on market data
        orderRef="defaultRef",
        tif="GTC",
        exchange="SMART",
        lastTradeDateOrContractMonth="",
        multiplier="0",
        outsideRth=False,
        # Specific to TBOT-WIND
        duration="4 W",
        bootup_duration="2 Y",
        end_date="",
    )
