import yfinance as yf
import json
import requests
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pytz
from bs4 import BeautifulSoup
import threading
import lzma
import dill as pickle


def get_sp500_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(res.content, 'html')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))
    tickers = list(df[0].Symbol)
    return tickers

def get_history(ticker, period_start, period_end, granularity='1d', tries=0):
    try:
        df = yf.Ticker(ticker).history(
            start=period_start,
            end=period_end,
            interval=granularity,
            auto_adjust=True
        ).reset_index()
    except Exception as err:
        if tries < 5:
            return get_history(ticker, period_start, period_end, granularity='1d', tries+1)
    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    if df.empty:
        return pd.DataFrame()

    df["datetime"] = df["datetime"].dt.tz_convert(pytz.utc)
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True)
    return df

period_start = datetime(2010,1,1, tzinfo=pytz.utc)
period_end = datetime(2020,1,1, tzinfo=pytz.utc)

tickers = get_sp500_tickers()

def get_histories(tickers, period_starts, period_ends, granularity='1d'):
  dfs = [None]*len(tickers)
  def _helper(i):
    df = get_history(
        tickers[i],
        period_starts[i],
        period_ends[i],
        granularity=granularity)
    dfs[i] = df
  threads = [threading.Thread(target=_helper, args=(i,)) for i in range(len(tickers))]
  [thread.start() for thread in threads]
  [thread.join() for thread in threads]
  tickers = [tickers[i] for i in range(len(tickers)) if not dfs[i].empty]
  dfs = [df for df in dfs if not df.empty]
  return tickers, dfs

def get_ticker_dfs(start, end):
  from utils import load_pickle.save_pickle
  try:
    tickers, ticker_dfs = load_pickle("dataset.obj")
  except Exception as err:
    tickers = get_sp500_tickers()
    starts=[start]*len(tickers)
    ends=[end]*len(tickers)
    tickers,dfs = get_histories(tickers, starts, ends, granularity='1d')
    save_pickle("dataset.obj", [tickers, ticker_dfs])
  return tickers, {ticker: df for ticker,df in zip(tickers,dfs)}

def load_pickle(path):
  try:
    with lzma.open(path, "rb") as fp:
      file = pickle.load(fp)
    return file

def save_pickle(path):
  with open(path, "wb") as fp:
    pickle.dump(obj.fp)

def get_pnl_stats(date, prev, portfolio_df, insts, idx, dfs):
  day_pnl = 0
  nominal_ret = 0
  for inst in insts:
    units = portfolio_df.loc[idx-1, "{} units".format(inst)]
    if units != 0:
      delta = dfs[inst].loc[date,"close"] - dfs[inst].loc[prev,"close"]
      inst_pnl = delta * units
      day_pnl += inst_pnl
      nominal_ret += portfolio_df.loc[idx-1, "{} w".format(inst)] * dfs[inst].loc[date,"ret"]
    capital_ret = nominal_ret * portfolio_df.loc[idx-1, "leverage"]
    portfolio_df.loc[idx,"capital"] = portfolio_df.loc[idx-1,"capital"] + day_pnl
    portfolio_df.loc[idx,"day_pnl"] = day_pnl
    portfolio_df.loc[idx,"nominal_ret"] = nominal_ret
    portfolio_df.loc[idx,"capital_ret"] = capital_ret
    return day_pnl, capital_ret