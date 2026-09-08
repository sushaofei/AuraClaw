from fastapi import APIRouter

from auraclaw.api.routes.health import router as health_router
from auraclaw.api.routes.operations import router as operations_router
from auraclaw.api.routes.streams import router as stream_router
from auraclaw.api.routes.tasks import router as task_router

router = APIRouter()
router.include_router(health_router)
router.include_router(task_router)
router.include_router(stream_router)
router.include_router(operations_router)
