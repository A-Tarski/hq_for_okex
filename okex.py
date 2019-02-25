import datetime
import zlib

from hyperquant.api import Platform, Sorting, Interval, Direction
from hyperquant.clients import WSClient, Endpoint, Trade, Error, \
    ParamName, WSConverter, RESTConverter, PrivatePlatformRESTClient, Candle, ItemObject


# REST

class OkexRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://www.okex.com/api/v{version}/"

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE_HISTORY: "trades.do",
        Endpoint.CANDLE: "kline.do",
    }

    param_name_lookup = {
        ParamName.SYMBOL: "symbol",
        ParamName.LIMIT: 'size',
        ParamName.IS_USE_MAX_LIMIT: None,
        ParamName.INTERVAL: "type",

        ParamName.TIMESTAMP: None,
        ParamName.FROM_ITEM: "since",
        ParamName.FROM_TIME: "since",

    }

    param_value_lookup = {
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,

        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.WEEK_1: "1week",
    }

    max_limit_by_endpoint = {
        Endpoint.TRADE_HISTORY: 60,
        Endpoint.CANDLE: 2000,
    }

    # For parsing

    param_lookup_by_class = {
        # Error
        Error: {
            "code": "code",
            "msg": "message",
        },
        # Data
        Trade: {
            "date_ms": ParamName.TIMESTAMP,
            "tid": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "type": ParamName.DIRECTION,
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT
        ],
    }


    # For converting time
    is_source_in_milliseconds = True

    # timestamp_platform_names = [ParamName.TIMESTAMP]

class OkexRESTClient(PrivatePlatformRESTClient):
    # Settings:
    platform_id = Platform.OKEX
    version = "1"

    _converter_class_by_version = {
        "1": OkexRESTConverterV1,
    }

    # ratelimit_error_in_row_count = 0
    is_source_in_timestring = True

    @property
    def headers(self):
        result = super().headers
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

# WebSocket

class OkexWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://real.okex.com:10440/ws/v1"
    is_source_in_milliseconds = True

    endpoint_lookup = {
        Endpoint.TRADE: '{symbol}:deals',
        Endpoint.CANDLE: '{symbol}:kline_{interval}',
    }

    param_lookup_by_class = {
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT,
            ParamName.INTERVAL,
            ParamName.SYMBOL
        ],
        Trade: [
            ParamName.ITEM_ID,
            ParamName.PRICE,
            ParamName.AMOUNT,
            ParamName.TIMESTAMP,
            ParamName.DIRECTION,
            ParamName.SYMBOL,
        ],
    }

    param_value_lookup = {
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.WEEK_1: "1week",
    }

    @staticmethod
    def convert_to_timestamp(time_str):
        h_m_s = list(map(int, time_str.split(':')))
        now_datetime = datetime.datetime.utcnow()
        deal_datetime = now_datetime.replace(hour=h_m_s[0],
                                             minute=h_m_s[1],
                                             second=h_m_s[2],
                                             microsecond=0)
        # Hong Kong Time
        deal_datetime -= datetime.timedelta(hours=8)
        timediff = deal_datetime - datetime.datetime(1970, 1, 1)
        return timediff.total_seconds() * 1000

    def _parse_item(self, endpoint, item_data):
        item_data = [self.to_float(item) for item in item_data]
        if endpoint == 'trade':
            item_data[3] = self.convert_to_timestamp(item_data[3])
        item = super()._parse_item(endpoint, item_data)
        if item and isinstance(item, ItemObject):
            if isinstance(item, Trade):
                item.direction = Direction.BUY if item.direction == "bid" else (
                    Direction.SELL if item.direction == "ask" else None)
        return item

    @staticmethod
    def to_float(string):
        try:
            return float(string)
        except ValueError:
            return string


    def parse(self, endpoint, data):
        # Skip list checking

        if not data:
            self.logger.warning("Data argument is empty in parse(). endpoint: %s, data: %s", endpoint, data)
            return data
        return self._parse_item(endpoint, data)

class OkexWSClient(WSClient):

    platform_id = Platform.OKEX
    version = "1"
    subscriptions_info = {} # fast track ws connections

    _converter_class_by_version = {
        "1": OkexWSConverterV1
    }

    def subscribe(self, endpoints=None, symbols=None, **params):
        if not endpoints and not symbols:
            self._subscribe(self.current_subscriptions)
        else:
            platform_params = {self.converter._get_platform_param_name(key):
                                  self.converter._process_param_value(key, value)
                               for key, value in params.items() if value is not None} if params else {}
            super().subscribe(endpoints, symbols, **platform_params)

    def _send_subscribe(self, subscriptions):
        for sub in subscriptions:
            sub_info = sub.split(':')
            event_data = {
                "event": "addChannel",
                "channel": 'ok_sub_spot_{}_{}'.format(*sub_info)}
            self.save_subscription(event_data["channel"], sub_info)
            self._send(event_data)

    def save_subscription(self, channel, info):
        # save subscription info to dict. Allow fast find subscription parameters

        endpoint = None
        interval = None
        self.subscriptions_info[channel] = []
        if 'kline' in info[1]:
            endpoint = Endpoint.CANDLE
            interval = info[1].split('_')[1]
            interval = self.convert_to_interval(interval)
        if 'deals' in info[1]:
            endpoint = Endpoint.TRADE
        if interval:
            self.subscriptions_info[channel] = [interval]
        self.subscriptions_info[channel].extend([info[0], endpoint])


    def convert_to_interval(self, param):
        # back conversion from platform value to Interval class

        for key, value in self.converter.param_value_lookup.items():
            if param == value:
                return key

    def _on_message(self, message):

        def inflate(data):
            #Encode Okex ws data

            decompress = zlib.decompressobj(
                    -zlib.MAX_WBITS  # see above
            )
            inflated = decompress.decompress(data)
            inflated += decompress.flush()
            return inflated

        message = inflate(message)

        super()._on_message(message.decode('utf-8'))

    def _parse(self, endpoint, data):
        if data:
            if isinstance(data, list): # standard okex ws message format
                data = data[0]
            endpoint, data = self._merge_channel_info(data)
            if 'data' in data:
                data = data['data']
        if not endpoint:
            return
        return super()._parse(endpoint, data)


    def _merge_channel_info(self, items_data):
        # append subscription information to ws message

        endpoint = None
        if 'channel' in items_data and 'ok_sub' in items_data['channel']:
            loaddata = list(self.subscriptions_info[items_data['channel']])
            endpoint = loaddata.pop()
            for item in items_data['data']:
                item.extend(loaddata)
        return endpoint, items_data
