from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router  # Make sure routes.py is correctly set up

# Create FastAPI app instance
app = FastAPI(title="Dynamic API")

# CORS Middleware to handle cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this for production 
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Include the router that contains your endpoint logic
app.include_router(router, prefix="/api", tags=["Dynamic Endpoints"])

@app.get("/")
def root():
    return {"message": "Dynamic API for Data Lake"}
