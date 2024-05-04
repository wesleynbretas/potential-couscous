#!/usr/bin/env python
# coding: utf-8

# In[29]:


from io import StringIO

import pandas as pd
from prefect import task, get_run_logger
import requests

#from utils import log


# In[30]:


@task(name="download_data")
def download_data(n_users: int) -> str:
    """
    Baixa dados da API https://randomuser.me e retorna um texto em formato CSV.

    Args:
        n_users (int): número de usuários a serem baixados.

    Returns:
        str: texto em formato CSV.
    """
    response = requests.get(
        "https://randomuser.me/api/?results={}&format=csv".format(n_users)
    )
    
    #logger = get_run_logger()
    #logger.info("Dados baixados com sucesso!")
    return response.text


# In[31]:


@task(name="parse_data")
def parse_data(data: str) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        data (str): texto em formato CSV.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """
    df = pd.read_csv(StringIO(data))

    #logger = get_run_logger()    
    #logger.info("Dados convertidos em Dataframe com sucesso!")    
    
    return df


# In[33]:


@task(name="save_report")
def save_report(dataframe: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """
    dataframe.to_csv("report.csv", index=False)
    
    # logger = get_run_logger()
    # logger.info("Dados salvos em report.csv com sucesso!")

