# -*- coding: utf-8 -*-

from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.data.trading_dates_mixin import TradingDatesMixin
from rqalpha.utils.exception import patch_user_exc
from rqalpha.utils.logger import system_log
from rqalpha.utils.datetime_func import convert_dt_to_int

import db

rq2gm = {'.XSHE': 'SZSE.', '.XSHG': 'SHSE.'}

class MinuteDataSource(BaseDataSource):
    def __init__(self, path):
        super(MinuteDataSource, self).__init__(path)
        self._cache = {}
        self._cache_size = 480 # should be larger than bar_count
        self._cached_dates = {}
        self.trading_dates_mixin = TradingDatesMixin(self.get_trading_calendar()) # TODO class field ?

    def _get_period_cache(self,order_book_id,start_dt,end_dt):
        df = self._cache[order_book_id]
        if not len(df): return df
        return df[(convert_dt_to_int(start_dt) <= df['datetime']) & (df['datetime']<= convert_dt_to_int(end_dt))]

    def _cache_count_bars(self,instrument,dt,bar_count,frequency='1m',fields=[],skip_suspended = True,adjust_type = 'pre', adjust_orig = None):
        if frequency != '1m':
            raise NotImplementedError

        if bar_count > self._cache_size:
            self._cache_size = bar_count

        order_book_id = instrument.order_book_id

        if order_book_id in self._cache:
            df = self._get_period_cache(order_book_id,datetime(dt.year,dt.month,dt.day),dt)
        else:
            df = pd.DataFrame()
            self._cached_dates[order_book_id] = []
        if dt.strftime('%H:%M') < '09:31':
            dtp = dt-timedelta(1)
        else:
            dtp = dt


        # it is ensured data in cache is continues in trading dates
        while len(df) < bar_count:
            start_dt = datetime(dtp.year, dtp.month, dtp.day)
            if not dtp.strftime('%Y-%m-%d') in self._cached_dates[order_book_id]:
                end_dt = datetime(dtp.year, dtp.month, dtp.day, 18)
                self._cache_period_bars(instrument, start_dt=start_dt,end_dt=end_dt)
            df = self._get_period_cache(order_book_id,start_dt,dt)
            dtp = self.trading_dates_mixin.get_previous_trading_date(dtp, 1).to_datetime()
            if dtp < instrument.listed_date: # TODO ensure escape from this loop while data is not avaliable
                break

    def _cache_period_bars(self,instrument,start_dt,end_dt,frequency='1m',fields=[],adjust_type = 'pre', adjust_orig = None):
        # # data at start_dt and end_dt are included
        if frequency != '1m':
            raise NotImplementedError

        order_book_id = instrument.order_book_id

        sec = rq2gm[order_book_id[-5:]] + order_book_id[:6]
        sec_dfs,trading_dates = db.get_data_GM(sec_list=[sec], start_dt=start_dt, end_dt=end_dt)
        if (not sec_dfs) or ( not len(sec_dfs[sec])):
            df = pd.DataFrame()
        else:
            df = sec_dfs[sec]
            df['datetime'] = df['datetime'].apply(lambda x: convert_dt_to_int(x))

        if order_book_id in self._cache:
            self._cache[order_book_id] = pd.concat([self._cache[order_book_id], df], ignore_index=True)
        else:
            self._cache[order_book_id] = df
        try:
            self._cached_dates[order_book_id] += trading_dates
            # self._cached_dates[order_book_id] = list(set(self._cached_dates[order_book_id]))
        except:
            self._cached_dates[order_book_id] = trading_dates

    def _sort_cache(self,order_book_id):
        # if order_book_id not in self._cache:
        #     return
        self._cache[order_book_id] = self._cache[order_book_id].sort_values(by='datetime', axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last')
        self._cached_dates[order_book_id].sort()

    def _shrink_cache(self,order_book_id):
        self._sort_cache(order_book_id)
        while (len(self._cache[order_book_id]) > self._cache_size and len(self._cached_dates[order_book_id]) > 1):
            df = self._cache[order_book_id]
            dt = convert_dt_to_int(datetime.strptime(self._cached_dates[order_book_id][1], '%Y-%m-%d'))
            self._cache[order_book_id] = df[df['datetime'] > dt]
            self._cached_dates[order_book_id] = self._cached_dates[order_book_id][1:]

    def get_bar(self, instrument, dt, frequency,fields=[],adjust_type = 'none',adjust_orig = None):
        # TODO return adjusted bars, added field 'limit_up', 'limit_down'
        if frequency == '1d':
            return super(MinuteDataSource, self).get_bar(instrument, dt, frequency)
        if frequency != '1m':
            raise NotImplementedError

        order_book_id = instrument.order_book_id

        if (order_book_id not in self._cache) or (dt.strftime('%Y-%m-%d') not in self._cached_dates[order_book_id]):
            self._cache_period_bars(instrument,start_dt=datetime(dt.year,dt.month,dt.day,9), end_dt=datetime(dt.year,dt.month,dt.day,18))
            self._shrink_cache(order_book_id) #TODO ensure shrink will not remove the wanted bar, if get_bar is always used to get the latest bar,this won't be a problem

        try:
            dtint = convert_dt_to_int(dt)
            df = self._cache[order_book_id]
            return df[df['datetime']==dtint].iloc[0].to_dict()
        except:
            return None

    def history_bars(self, instrument, bar_count, frequency, fields, dt, skip_suspended=True, include_now=False,
                     adjust_type='pre', adjust_orig=None):
        # TODO return adjusted bars, added field 'limit_up', 'limit_down'
        if frequency == '1d':
            return super(MinuteDataSource, self).history_bars(instrument, bar_count, frequency, fields, dt,
         skip_suspended, include_now,adjust_type, adjust_orig)

        if frequency != '1m':
                raise NotImplementedError

        self._cache_count_bars(instrument=instrument,dt=dt,bar_count=bar_count)
        self._sort_cache(instrument.order_book_id)
        df = self._cache[instrument.order_book_id]
        df = df[df['datetime']<= convert_dt_to_int(dt)]
        if len(df) > bar_count:
            df = df[-bar_count:]
        return df

    def available_data_range(self, frequency):
        return date(2017, 5, 1), date.today() - relativedelta(days=1)
