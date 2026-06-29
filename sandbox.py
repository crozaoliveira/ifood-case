from src.utils import generate_year_month_range
from src.config import (DATE_FROM, DATE_TO, VOLUME_PATH, JOB_RUN_ID, TASK_RUN_ID, TASK_KEY)

if __name__ == "__main__":
    # Testando a função parse_year_month

    result = generate_year_month_range(DATE_FROM, DATE_TO)
    print(f"Job: {JOB_RUN_ID} / Task: {TASK_KEY} / Run: {TASK_RUN_ID}")
    print(f"Período gerado entre {DATE_FROM} e {DATE_TO}: {result}")
    print(f"Dados salvos em: {VOLUME_PATH}")