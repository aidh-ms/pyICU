"""Module establishing database connection strings and engine."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


load_dotenv()

POSTGRES_DIALECT: str | None = os.getenv("POSTGRES_DIALECT")
POSTGRES_DRIVER: str | None = os.getenv("POSTGRES_DRIVER")
POSTGRES_USER: str | None = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD: str | None = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST: str | None = os.getenv("POSTGRES_HOST")
POSTGRES_PORT: str | None = os.getenv("POSTGRES_PORT")
MIMIC_DATABASE: str | None = os.getenv("MIMIC_DATABASE")
MIMIC_DEMO_DATABASE: str | None = os.getenv("MIMIC_DEMO_DATABASE")

MIMIC_DATABASE_URL: str = f"{POSTGRES_DIALECT}+{POSTGRES_DRIVER}://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DATABASE}"
MIMIC_DEMO_DATABASE_URL: str = f"{POSTGRES_DIALECT}+{POSTGRES_DRIVER}://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DEMO_DATABASE}"

mimic_engine = create_engine(MIMIC_DATABASE_URL)
mimic_demo_engine = create_engine(MIMIC_DEMO_DATABASE_URL)
