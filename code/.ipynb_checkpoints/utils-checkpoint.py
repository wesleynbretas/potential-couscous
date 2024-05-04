#!/usr/bin/env python
# coding: utf-8

# In[1]:


import prefect


# In[2]:


def log(msg):
    prefect.context.logger.info(f"\n{msg}")

