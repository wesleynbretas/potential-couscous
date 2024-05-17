#!/usr/bin/env python
# coding: utf-8

# In[14]:


import pandas as pd
import pyarrow as pa
import yfinance as yf
from datetime import timedelta
from prefect import flow, task


# In[15]:


@task(retries=3, cache_expiration=timedelta(30))
def fetch_data(ticker):
    return yf.download(ticker)


# In[16]:


@task
def save_data(stock_df):
    stock_df.to_parquet()


# In[17]:


@flow
def pipeline(ticker):
    df = fetch_data(ticker)
    save_data(df)


# In[18]:


if __name__ == '__main__':
    pipeline(ticker="AAPL")


# In[ ]:




