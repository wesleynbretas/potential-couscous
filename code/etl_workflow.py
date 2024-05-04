#!/usr/bin/env python
# coding: utf-8

# In[21]:


from prefect import task, flow
from etl import extract_data, transform_data, load_data


# In[22]:


@task(name="extract_data_task")
def extract_data_task():
    return extract_data()


# In[23]:


@task(name="transform_data_task")
def transform_data_task(data):
    return transform_data(data)


# In[24]:


@task(name="load_data_task")
def load_data_task(transformed_data):
    return load_data(transformed_data)


# In[25]:


@flow(name="etl_workflow")
def etl_workflow():
    extracted_data = extract_data_task()
    transformed_data = transform_data_task(extracted_data)
    load_data_task(transformed_data)


# In[26]:


if __name__ == "__main__":
    etl_workflow()


# In[ ]:




