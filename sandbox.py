from src.utils import generate_year_month_range

if __name__ == "__main__":
    # Testando a função parse_year_month
    DATE_FROM = "2023/06"
    DATE_TO = "2023/05"

    result = generate_year_month_range(DATE_FROM, DATE_TO)
    print(f"Período gerado entre {DATE_FROM} e {DATE_TO}: {result}")