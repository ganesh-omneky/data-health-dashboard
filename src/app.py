import pandas as pd
import streamlit as st
from tqdm import tqdm

from src import s3
from src.async_manager import AsyncAPIManager, CustomUnit
from src.model import AdvertisementChannel
from src.sql import engine, sql_manager


def main():
    try:
        st.title("Brand Data Import Status Dashboard")
        # statistics = sql_manager.get_insights_stats()
        # df = pd.DataFrame(statistics)
        # st.table(data=df)

        # results = async_manager.run()
        # rows = []
        # for result in results:
        #     rows.extend(result)
        # st.table(data=rows)

        html = s3.read_html_from_s3("omneky-airbyte-sync", "insights_stats.html")
        st.markdown(html, unsafe_allow_html=True)
    finally:
        if engine.tunnel_forwarder:
            engine.tunnel_forwarder.stop()


if __name__ == "__main__":
    main()
