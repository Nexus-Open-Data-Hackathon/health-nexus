"""
CMS — Application entry point
===============================
Registers all routers and wires up startup/shutdown lifecycle.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings

# Import ALL models so Alembic's autogenerate sees every table
from models.core import Facility, District, Medicine, ICD10Code  # noqa: F401
from models.surveillance import DispensingRecord, SurveillanceAlert  # noqa: F401

from routers.facilities import router as facilities_router
from routers.search import router as search_router
from routers.surveillance import router as surveillance_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Table creation is handled by Alembic migrations.
    # Nothing to do here on startup.
    yield


app = FastAPI(
    title="CMS Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
    debug=settings.DEBUG,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(facilities_router)
app.include_router(search_router)
app.include_router(surveillance_router)


@app.get("/", tags=["Health"])
def root():
    return {
        "status": "ok",
        "env": settings.APP_ENV,
        "message": "CMS API is running.",
    }