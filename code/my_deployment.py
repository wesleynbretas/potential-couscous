#!/usr/bin/env python
# coding: utf-8

# In[4]:


from demo import pipeline
from prefect.deployments import Deployment


# In[5]:


deployment = Deployment.build_from_flow(
    flow=pipeline,
    name="Python Deployment Example"
)


# In[ ]:


if __name__ == "__main__":
    deployment.apply()

