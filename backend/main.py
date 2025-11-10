from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime

app = FastAPI(title="SAS Viya JES Bridge API")

# Allow frontend dev origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_ddmmmyyyy(value: str) -> datetime:
    try:
        return datetime.strptime(value.strip(), "%d%b%Y")
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid date format for '{value}'. Use DDMMMYYYY (e.g., 31AUG2019)")


class RunSASRequest(BaseModel):
    report_date: str = Field(..., example="31AUG2019")
    previous_date: Optional[str] = Field(None, example="31JUL2019")
    lcr_lines: str = Field(..., example="6,17")
    country: str = Field(..., example="SG")

    @validator("report_date")
    def validate_report_date(cls, v: str) -> str:
        parse_ddmmmyyyy(v)
        return v

    @validator("previous_date", always=True)
    def validate_previous_date(cls, v: Optional[str], values) -> Optional[str]:
        if v is None or v.strip() == "":
            return None
        parse_ddmmmyyyy(v)
        return v

    @validator("lcr_lines")
    def validate_lcr_lines(cls, v: str) -> str:
        parts = [p.strip() for p in v.split(",") if p.strip()]
        if not parts:
            raise ValueError("At least one LCR line is required")
        for p in parts:
            if not p.isdigit():
                raise ValueError("LCR lines must be comma-separated integers")
        return ",".join(parts)

    @validator("country")
    def validate_country(cls, v: str) -> str:
        vv = v.strip().upper()
        if len(vv) not in (2, 3):
            raise ValueError("Country must be ISO code (2 or 3 letters)")
        return vv


class SASRow(BaseModel):
    line: int
    metric: str
    value: float


class RunSASResponse(BaseModel):
    columns: List[str]
    rows: List[SASRow]
    meta: dict


@app.post("/run", response_model=RunSASResponse)
async def run_sas_job(payload: RunSASRequest):
    # In a real SAS Viya integration, here you'd submit a job to JES and wait for results.
    # For this sandbox, we simulate ETL and produce tabular results based on inputs.
    rd = parse_ddmmmyyyy(payload.report_date)
    pd = parse_ddmmmyyyy(payload.previous_date) if payload.previous_date else None

    lines = [int(x) for x in payload.lcr_lines.split(",")]

    rows: List[SASRow] = []
    for idx, ln in enumerate(lines, start=1):
        base = float(ln * 100)
        adj = (rd.toordinal() % 31) * 1.0
        prev_adj = (pd.toordinal() % 31) * 1.0 if pd else 0.0
        rows.append(SASRow(line=ln, metric="Current", value=base + adj))
        if pd:
            rows.append(SASRow(line=ln, metric="Previous", value=base + prev_adj))
            rows.append(SASRow(line=ln, metric="Delta", value=(adj - prev_adj)))

    return RunSASResponse(
        columns=["line", "metric", "value"],
        rows=rows,
        meta={
            "report_date": payload.report_date,
            "previous_date": payload.previous_date,
            "lcr_lines": payload.lcr_lines,
            "country": payload.country,
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok"}
