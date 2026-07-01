from src.config import DATE_FROM, DATE_TO
from src.ingestor import ingest_months
from src.utils import generate_year_month_range


if __name__ == "__main__":
    ingest_months(generate_year_month_range(DATE_FROM, DATE_TO))
