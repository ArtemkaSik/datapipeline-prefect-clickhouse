from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


@dataclass(frozen=True)
class DateState:
    current_date: date
    min_date: date
    max_date: date


def _parse_date(value: str, field_name: str, env_path: Path) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Invalid {field_name} in {env_path}. Expected YYYY-MM-DD, got {value!r}."
        ) from exc


def _read_env_file(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        raise FileNotFoundError(f"Date state file not found: {env_path}")

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid line in {env_path}: {raw_line!r}")
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def load_date_state(env_path: str | Path) -> DateState:
    path = Path(env_path)
    values = _read_env_file(path)

    current_date = _parse_date(values["CURRENT_DATE"], "CURRENT_DATE", path)
    min_date = _parse_date(values["MIN_DATE"], "MIN_DATE", path)
    max_date = _parse_date(values["MAX_DATE"], "MAX_DATE", path)

    if min_date > max_date:
        raise ValueError(f"MIN_DATE must be less than or equal to MAX_DATE in {path}")

    if current_date < min_date or current_date > max_date:
        raise ValueError(
            f"CURRENT_DATE must be inside [{min_date}, {max_date}] in {path}"
        )

    return DateState(
        current_date=current_date,
        min_date=min_date,
        max_date=max_date,
    )


def get_next_date(state: DateState) -> date:
    next_date = state.current_date + timedelta(days=1)
    if next_date > state.max_date:
        return state.min_date
    return next_date


def save_next_date(env_path: str | Path) -> date:
    path = Path(env_path)
    state = load_date_state(path)
    next_date = get_next_date(state)

    lines: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.strip().startswith("CURRENT_DATE="):
            lines.append(f"CURRENT_DATE={next_date.isoformat()}")
        else:
            lines.append(raw_line)

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return next_date
