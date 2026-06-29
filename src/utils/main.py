from datetime import datetime, timezone


def parse_year_month(value: str) -> tuple[int, int]:
    """
    Converte uma string no formato YYYY/MM para uma tupla (year, month).

    Exemplo:
        "2023/05" -> (2023, 5)
    """
    try:
        year_str, month_str = value.strip().split("/")
        year = int(year_str)
        month = int(month_str)
    except ValueError as error:
        raise ValueError(
            f"Data inválida: {value}. Use o formato YYYY/MM, exemplo: 2023/05."
        ) from error

    if month < 1 or month > 12:
        raise ValueError(
            f"Mês inválido em {value}. O mês deve estar entre 01 e 12."
        )

    return year, month


def generate_year_month_range(date_from: str, date_to: str) -> list[tuple[int, int]]:
    """
    Gera uma lista de meses entre DATE_FROM e DATE_TO, incluindo as duas pontas.

    Exemplo:
        DATE_FROM=2022/10
        DATE_TO=2023/02

        Retorno:
        [
            (2022, 10),
            (2022, 11),
            (2022, 12),
            (2023, 1),
            (2023, 2)
        ]
    """
    start_year, start_month = parse_year_month(date_from)
    end_year, end_month = parse_year_month(date_to)

    start_value = start_year * 12 + start_month
    end_value = end_year * 12 + end_month

    if start_value > end_value:
        raise ValueError(
            f"DATE_FROM não pode ser maior que DATE_TO. "
            f"Recebido: DATE_FROM={date_from}, DATE_TO={date_to}."
        )

    periods = []

    current_year = start_year
    current_month = start_month

    while (current_year * 12 + current_month) <= end_value:
        periods.append((current_year, current_month))

        current_month += 1

        if current_month > 12:
            current_month = 1
            current_year += 1

    return periods


def generate_execution_id(prefix: str = "run") -> str:
    timestamp = int(datetime.now(timezone.utc).timestamp())
    return f"{prefix}_{timestamp}"
