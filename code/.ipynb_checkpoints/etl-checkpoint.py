#!/usr/bin/env python
# coding: utf-8

# In[43]:


import pandas as pd
from os import path


# In[44]:


def extract_data():
    # read CSV file into DataFrame
    csv_path = path.join("data", "csv", "username.csv")
    
    data = pd.read_csv(csv_path, sep=";")

    return data


# In[45]:


def transform_data(data):
    data.rename(columns={"Username" : "username",
                         "Identifier" : "identifier",
                         "First name" : "first_name",
                         "Last name" : "last_name"
                        }, inplace=True)

    return data


# In[46]:


def load_data(data):
    csv_path = path.join("data", "csv", "username_data.csv")
    
    data.to_csv(csv_path, index=False)

