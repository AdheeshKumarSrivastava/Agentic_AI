from __future__ import annotations

import streamlit as st
from dotenv import load_dotenv

from config import settings
from observability.logger import configure_logging
from ui.pages import render_app

load_dotenv()
configure_logging(settings)

st.set_page_config(page_title=settings.APP_NAME, layout="wide")
render_app(settings)
