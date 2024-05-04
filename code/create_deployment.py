#!/usr/bin/env python
# coding: utf-8

# In[7]:


from prefect import flow


# In[8]:


if __name__ == "__main__":
    flow.from_source(
            source="https://github.com/wesleynbretas/potential-couscous.git",
            entrypoint="code/etl_workflow.py:etl_workflow",
        ).deploy(
            name="my-first-deployment",
            work_pool_name="my-managed-pool",
            cron="0 1 * * *",
        )


# In[ ]:




