from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from .api import tasks, tyc, config, qxb
import os

app = FastAPI()

# CORS - allow frontend dev server or any origin in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# mount routers
app.include_router(tasks.router)
app.include_router(tyc.router)
app.include_router(config.router)
app.include_router(qxb.router)

# serve built frontend if exists
frontend_dist = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'frontend', 'dist'))
if os.path.isdir(frontend_dist):
    app.mount('/', StaticFiles(directory=frontend_dist, html=True), name='frontend')

@app.get('/')
async def root():
    return {"message": "FinanceKG FastAPI backend running"}
