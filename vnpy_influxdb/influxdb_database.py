""""""
import ast
from datetime import datetime
from typing import List
import shelve

import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions, SYNCHRONOUS, WritePrecision

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, OrderData, TradeData, PositionData, AccountData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import (
    generate_vt_symbol,
    extract_vt_symbol,
    get_file_path
)

import json, re
import numpy as np
import pytz


class InfluxdbDatabase(BaseDatabase):
    """InfluxDB数据库接口"""
    # overview_filename = "influxdb_overview"
    # overview_filepath = str(get_file_path(overview_filename))

    def __init__(self) -> None:
        """"""
        # database = SETTINGS["database.database"]
        # user = SETTINGS["database.user"]
        # password = SETTINGS["database.password"]
        # host = SETTINGS["database.host"]
        # port = SETTINGS["database.port"]

        url = SETTINGS["database.url"]
        token = SETTINGS["database.token"]
        self.org = SETTINGS["database.org"]
        self.bucket = SETTINGS["database.bucket"]

        with open(SETTINGS['database.tableconf'], 'r') as f:
            self.db_table_conf = json.loads(f.read())

        self.client = InfluxDBClient(url=url, token=token)
        # self.bucket_api = self.client.buckets_api()
        # if self.bucket_api.find_bucket_by_name(self.bucket) is None:
        #     self.bucket_api.create_bucket(bucket_name=self.bucket)

        # self.write_api = self.client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000))
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        # self.client = InfluxDBClient(host, port, user, password, database)
        # self.client.create_database(database)

        self.client = InfluxDBClient(url=url, token=token)
        # self.bucket_api = self.client.buckets_api()
        # if self.bucket_api.find_bucket_by_name(self.bucket) is None:
        #     self.bucket_api.create_bucket(bucket_name=self.bucket)

        # self.write_api = self.client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000))
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        # self.client = InfluxDBClient(host, port, user, password, database)
        # self.client.create_database(database)

    def save_bar_data(self, bars: List[BarData], stream: bool = False) -> bool:
        """保存K线数据"""
        json_body = []
        # 读取主键参数
        bar = bars[0]
        vt_symbol = bar.vt_symbol
        interval = bar.interval

        # 将BarData数据转换为字典，并调整时区
        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d: dict = {
                "measurement": "bar_data",
                "tags": {
                    "vt_symbol": vt_symbol,
                    "interval": interval.value
                },
                "time": bar.datetime.isoformat(),
                "fields": {
                    "open_price": float(bar.open_price),
                    "high_price": float(bar.high_price),
                    "low_price": float(bar.low_price),
                    "close_price": float(bar.close_price),
                    "volume": float(bar.volume),
                    "turnover": float(bar.turnover),
                    "open_interest": float(bar.open_interest),
                }
            }
            json_body.append(d)

        self.client.write_points(json_body, batch_size=10000)

        # 更新K线汇总数据
        symbol, exchange = extract_vt_symbol(vt_symbol)
        key: str = f"{vt_symbol}_{interval.value}"

        f: shelve.DbfilenameShelf = shelve.open(self.bar_overview_filepath)
        overview = f.get(key, None)

        if not overview:
            overview = BarOverview(
                symbol=symbol,
                exchange=exchange,
                interval=interval
            )
            overview.count = len(bars)
            overview.start = bars[0].datetime
            overview.end = bars[-1].datetime
        elif stream:
            overview.end = bars[-1].datetime
            overview.count += len(bars)
        else:
            overview.start = min(overview.start, bars[0].datetime)
            overview.end = max(overview.end, bars[-1].datetime)

            query = (
                "select count(close_price) from bar_data"
                " where vt_symbol=$vt_symbol"
                " and interval=$interval"
            )
            bind_params = {
                "vt_symbol": vt_symbol,
                "interval": interval.value
            }
            result = self.client.query(query, bind_params=bind_params)
            points = result.get_points()

            for d in points:
                overview.count = d["count"]

        f[key] = overview
        f.close()

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        points = []

        for tick in ticks:

            tick.datetime = convert_tz(tick.datetime)
            vt_symbol = tick.vt_symbol
            exchange = tick.exchange.value

            vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            vnpy_tick_fields['date'] = int(tick.datetime.strftime("%Y%m%d"))
            vnpy_tick_fields['time'] = int(tick.datetime.strftime("%H%M%S%f"))

            tick.datetime = convert_tz(tick.datetime)
            vt_symbol = tick.vt_symbol
            exchange = tick.exchange.value
            if tick.localtime is None:
                localtime = "Nan"
            else:
                localtime = tick.localtime

            vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            vnpy_tick_fields['date'] = int(tick.datetime.strftime("%Y%m%d"))
            vnpy_tick_fields['time'] = int(tick.datetime.strftime("%H%M%S%f"))

            vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            vnpy_tick_fields['dateint'] = int(tick.datetime.strftime("%Y%m%d"))
            vnpy_tick_fields['timeint'] = int(tick.datetime.strftime("%H%M%S%f"))
            vnpy_tick_fields['datetimeiso'] = tick.datetime.isoformat()
            vnpy_tick_fields['localtime'] = tick.localtime.timestamp()

            d: dict = {
                "measurement": "tick_data",
                "tags": {
                    "symbol": vt_symbol,
                    "exchange": exchange
                },
                "time": tick.datetime.astimezone(pytz.timezone('Asia/Shanghai')),
                "fields": vnpy_tick_fields
            }
            points.append(Point.from_dict(d))

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)


    # TODO add save order
    # TODO add save trade
    # TODO add save position
    # TODO add save account
    # load tick data

    def save_order_data(self, orders: List[OrderData]) -> bool:
        points = []

        for order in orders:
            order.datetime = convert_tz(order.datetime)
            vt_symbol = order.vt_symbol
            exchange = order.exchange.value

            # vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            # vnpy_tick_fields['date'] = int(tick.datetime.strftime("%Y%m%d"))
            # vnpy_tick_fields['time'] = int(tick.datetime.strftime("%H%M%S%f"))

            # d = {
            #     "measurement": "order_data",
            #     "tags": {
            #         "symbol": vt_symbol,
            #         "exchange": exchange
            #     },
            #     "time": order.datetime.astimezone(pytz.timezone('Asia/Shanghai')),
            #     "fields": vnpy_tick_fields
            # }
            # points.append(Point.from_dict(d))

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)

    def save_trade_data(self, trades: List[TradeData]) -> bool:
        points = []

        for trade in trades:
            trade.datetime = convert_tz(trade.datetime)
            vt_symbol = trade.vt_symbol
            exchange = trade.exchange.value

            # vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            # vnpy_tick_fields['date'] = int(tick.datetime.strftime("%Y%m%d"))
            # vnpy_tick_fields['time'] = int(tick.datetime.strftime("%H%M%S%f"))

            # d = {
            #     "measurement": "trade_data",
            #     "tags": {
            #         "symbol": vt_symbol,
            #         "exchange": exchange
            #     },
            #     "time": trade.datetime.astimezone(pytz.timezone('Asia/Shanghai')),
            #     "fields": vnpy_tick_fields
            # }
            # points.append(Point.from_dict(d))

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)

    def save_position_data(self, positions: List[PositionData]) -> bool:
        points = []

        for position in positions:
            position.datetime = convert_tz(position.datetime)
            vt_symbol = position.vt_symbol
            exchange = position.exchange.value

            # vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            # vnpy_tick_fields['date'] = int(tick.datetime.strftime("%Y%m%d"))
            # vnpy_tick_fields['time'] = int(tick.datetime.strftime("%H%M%S%f"))

            # d = {
            #     "measurement": "position_data",
            #     "tags": {
            #         "symbol": vt_symbol,
            #         "exchange": exchange
            #     },
            #     "time": position.datetime.astimezone(pytz.timezone('Asia/Shanghai')),
            #     "fields": vnpy_tick_fields
            # }
            # points.append(Point.from_dict(d))

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)

    def save_account_data(self, accounts: List[AccountData]) -> bool:
        points = []

        for account in accounts:
            account.datetime = convert_tz(account.datetime)
            vt_symbol = account.vt_symbol
            exchange = account.exchange.value

            # vnpy_tick_fields = {n: getattr(tick, n, np.nan) for n in self.db_table_conf['vnpy']['tick']}
            # vnpy_tick_fields['date'] = int(tick.datetime.strftime("%Y%m%d"))
            # vnpy_tick_fields['time'] = int(tick.datetime.strftime("%H%M%S%f"))

            # d = {
            #     "measurement": "account_data",
            #     "tags": {
            #         "symbol": vt_symbol,
            #         "exchange": exchange
            #     },
            #     "time": account.datetime.astimezone(pytz.timezone('Asia/Shanghai')),
            #     "fields": vnpy_tick_fields
            # }
            # points.append(Point.from_dict(d))

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)



    def load_bar_data(
            self,
            symbol: str,
            exchange: Exchange,
            interval: Interval,
            start: datetime,
            end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        query = (
            "select * from bar_data"
            " where vt_symbol=$vt_symbol"
            " and interval=$interval"
            f" and time >= '{start.date().isoformat()}'"
            f" and time <= '{end.date().isoformat()}';"
        )

        bind_params = {
            "vt_symbol": generate_vt_symbol(symbol, exchange),
            "interval": interval.value
        }

        result = self.client.query(query, bind_params=bind_params)
        points = result.get_points()

        bars: List[BarData] = []
        for d in points:
            dt = datetime.strptime(d["time"], "%Y-%m-%dT%H:%M:%SZ")

            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                datetime=datetime.fromtimestamp(dt.timestamp(), DB_TZ),
                open_price=d["open_price"],
                high_price=d["high_price"],
                low_price=d["low_price"],
                close_price=d["close_price"],
                volume=d["volume"],
                turnover=d["turnover"],
                open_interest=d["open_interest"],
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        bind_params = {
            "vt_symbol": generate_vt_symbol(symbol, exchange),
            "interval": interval.value
        }

        # 查询数量
        query1 = (
            "select count(close_price) from bar_data"
            " where vt_symbol=$vt_symbol"
            " and interval=$interval"
        )
        result = self.client.query(query1, bind_params=bind_params)
        points = result.get_points()

        for d in points:
            count = d["count"]

        # 删除K线数据
        query2 = (
            "drop series from bar_data"
            " where vt_symbol=$vt_symbol"
            " and interval=$interval"
        )
        self.client.query(query2, bind_params=bind_params)

        # 删除K线汇总数据
        f: shelve.DbfilenameShelf = shelve.open(self.bar_overview_filepath)
        vt_symbol = generate_vt_symbol(symbol, exchange)
        key = f"{vt_symbol}_{interval.value}"

        if key in f:
            f.pop(key)
        f.close()

        return count

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime,
        fields: list
    ) -> List[TickData]:
        """读取TICK数据"""

        query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: start, stop: end)
            |> filter(fn: (r) => r["_measurement"] == "tick_data")
            |> filter(fn: (r) => r["symbol"] == "{symbol}")
            |> filter(fn: (r) => r["exchange"] == "{exchange}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        if len(fields) > 0:
            fields_str = ','.join([f'"{f}"' for f in fields])
            query += f"""|> keep(columns: [{fields_str}])"""

        bind_params = {
            "start": start,
            "end": end,
        }

        tables = self.query_api.query(query, params=bind_params, org=self.org)
        ticks_df = pd.DataFrame([record.values for table in tables for record in table.records])

        return ticks_df

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        bind_params = {
            "vt_symbol": generate_vt_symbol(symbol, exchange),
        }

        # 查询TICK数量
        query1 = (
            "select count(last_price) from tick_data"
            " where vt_symbol=$vt_symbol"
        )
        result = self.client.query(query1, bind_params=bind_params)
        points = result.get_points()

        for d in points:
            count = d["count"]

        # 删除TICK数据
        query2 = (
            "drop series from tick_data"
            " where vt_symbol=$vt_symbol"
        )
        self.client.query(query2, bind_params=bind_params)

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """查询数据库中的K线汇总信息"""
        # 如果已有K线，但缺失汇总信息，则执行初始化
        query = "select count(close_price) from bar_data"
        result = self.client.query(query)
        points = result.get_points()
        data_count = 0
        for d in points:
            data_count = d["count"]

        f = shelve.open(self.overview_filepath)
        overview_count = len(f)

        if data_count and not overview_count:
            self.init_bar_overview()

        overviews = list(f.values())
        f.close()
        return overviews


database_manager = InfluxdbDatabase()
