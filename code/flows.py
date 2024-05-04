#!/usr/bin/env python
# coding: utf-8

# In[22]:


from prefect import flow


# In[23]:


from tasks import (
    download_data,
    parse_data,
    save_report,
)


# In[25]:


@flow(name="process_data")
def process_data(n_users):
    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    save_report(dataframe)

