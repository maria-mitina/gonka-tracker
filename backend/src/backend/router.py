from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Any
from backend.models import InferenceResponse, ParticipantDetailsResponse, TimelineResponse, ModelsResponse, ParticipantInferencesResponse

router = APIRouter(prefix="/v1")

inference_service: Optional[Any] = None


def set_inference_service(service):
    global inference_service
    inference_service = service


@router.get("/hello")
def hello():
    return {"message": "hello"}


@router.get("/inference/current", response_model=InferenceResponse)
async def get_current_inference_stats(reload: bool = False):
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        return await inference_service.get_current_epoch_stats(reload=reload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch current epoch stats: {str(e)}")


@router.get("/inference/epochs/{epoch_id}", response_model=InferenceResponse)
async def get_epoch_inference_stats(epoch_id: int, height: Optional[int] = None):
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    if epoch_id < 1:
        raise HTTPException(status_code=400, detail="Invalid epoch ID")
    
    if height is not None and height < 1:
        raise HTTPException(status_code=400, detail="Invalid height")
    
    try:
        return await inference_service.get_historical_epoch_stats(epoch_id, height=height)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch epoch {epoch_id} stats: {str(e)}")


@router.get("/participants/{participant_id}", response_model=ParticipantDetailsResponse)
async def get_participant_details(
    participant_id: str,
    epoch_id: int = Query(..., description="Epoch ID (required)"),
    height: Optional[int] = Query(None, description="Block height (optional)")
):
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    if epoch_id < 1:
        raise HTTPException(status_code=400, detail="Invalid epoch ID")
    
    if height is not None and height < 1:
        raise HTTPException(status_code=400, detail="Invalid height")
    
    try:
        details = await inference_service.get_participant_details(
            participant_id=participant_id,
            epoch_id=epoch_id,
            height=height
        )
        
        if details is None:
            raise HTTPException(
                status_code=404,
                detail=f"Participant {participant_id} not found in epoch {epoch_id}"
            )
        
        return details
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch participant details: {str(e)}"
        )


@router.get("/timeline", response_model=TimelineResponse)
async def get_timeline():
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        return await inference_service.get_timeline()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch timeline: {str(e)}")


@router.get("/models/current", response_model=ModelsResponse)
async def get_current_models():
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        return await inference_service.get_current_models()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch current models: {str(e)}")


@router.get("/models/epochs/{epoch_id}", response_model=ModelsResponse)
async def get_historical_models(epoch_id: int, height: Optional[int] = None):
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    if epoch_id < 1:
        raise HTTPException(status_code=400, detail="Invalid epoch ID")
    
    if height is not None and height < 1:
        raise HTTPException(status_code=400, detail="Invalid height")
    
    try:
        return await inference_service.get_historical_models(epoch_id, height)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch models for epoch {epoch_id}: {str(e)}")


@router.get("/participants/{participant_id}/inferences", response_model=ParticipantInferencesResponse)
async def get_participant_inferences(
    participant_id: str,
    epoch_id: int = Query(..., description="Epoch ID (required)")
):
    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    if epoch_id < 1:
        raise HTTPException(status_code=400, detail="Invalid epoch ID")
    
    try:
        inferences = await inference_service.get_participant_inferences_summary(
            epoch_id=epoch_id,
            participant_id=participant_id
        )
        return inferences
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch inferences: {str(e)}"
        )


@router.post("/test/alert")
async def test_alert():
    from backend.email_alert import EmailAlert
    from backend.webhook_alert import WebhookAlert
    
    email_alert = EmailAlert()
    webhook_alert = WebhookAlert()
    
    subject = "Test Alert: Block Height Growth Stagnation"
    message = (
        "This is a test alert to verify the notification system is working.\n\n"
        "Current block height: TEST\n"
        "Last growth detected: TEST seconds ago\n"
        "Timestamp: TEST"
    )
    
    results = {}
    
    if email_alert.enabled:
        results["email"] = await email_alert.send_alert(subject, message)
    else:
        results["email"] = "not configured"
    
    if webhook_alert.enabled:
        results["webhook"] = await webhook_alert.send_alert(subject, message)
    else:
        results["webhook"] = "not configured"
    
    return {
        "message": "Test alert sent",
        "results": results
    }


@router.get("/test/block-height-status")
async def get_block_height_status():
    import time
    import backend.app as app_module

    if inference_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        sync_info = await inference_service.client.get_status_sync_info()
        current_height = sync_info["latest_block_height"]
        latest_block_time = sync_info.get("latest_block_time")
        now = time.time()
        time_since_last_block_seconds = int(now - latest_block_time) if latest_block_time is not None else None
        threshold = app_module.BLOCK_HEIGHT_ALERT_THRESHOLD
        return {
            "current_height": current_height,
            "time_since_last_block_seconds": time_since_last_block_seconds,
            "alert_threshold_seconds": threshold,
            "would_alert": time_since_last_block_seconds is not None and time_since_last_block_seconds >= threshold,
            "monitoring_active": True,
            "note": "Alert fires when time_since_last_block_seconds >= alert_threshold_seconds.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get block height: {str(e)}")


@router.post("/test/simulate-block-stagnation")
async def simulate_block_stagnation():
    """Simulate block stagnation by fixing the height at current value for testing"""
    import backend.app as app_module
    
    if app_module.inference_service_instance is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        current_height = await app_module.inference_service_instance.client.get_latest_height()
        
        app_module.block_height_test_mode = True
        app_module.block_height_test_fixed_height = current_height
        
        return {
            "message": f"Block stagnation simulation enabled. Height fixed at {current_height}",
            "fixed_height": current_height,
            "note": "Monitoring will now see the same height repeatedly, triggering alert after threshold. Use POST /v1/test/disable-block-stagnation to disable."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to simulate stagnation: {str(e)}")


@router.post("/test/disable-block-stagnation")
async def disable_block_stagnation():
    """Disable block stagnation simulation"""
    import backend.app as app_module
    
    app_module.block_height_test_mode = False
    app_module.block_height_test_fixed_height = None
    
    return {
        "message": "Block stagnation simulation disabled. Monitoring will use real block heights.",
        "test_mode": False
    }


@router.get("/test/epoch-fetch-status")
async def get_epoch_fetch_status():
    """Return consecutive get_current_epoch_stats failures and whether Chain/API unreachable alert would fire."""
    import backend.app as app_module

    if app_module.inference_service_instance is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    consecutive = app_module.consecutive_epoch_fetch_failures
    threshold = app_module.EPOCH_FETCH_ALERT_CONSECUTIVE_THRESHOLD
    return {
        "consecutive_failures": consecutive,
        "alert_threshold": threshold,
        "would_alert": consecutive >= threshold,
        "note": "Alert fires when get_current_epoch_stats fails this many times in a row. Resets on next success.",
    }


@router.get("/test/rewards-alert-status")
async def get_rewards_alert_status():
    """Return rewards count in the alert window and whether a no-rewards alert would fire."""
    import backend.app as app_module

    if app_module.postgres_db_instance is None:
        raise HTTPException(status_code=503, detail="PostgreSQL not initialized")

    try:
        window_hours = app_module.REWARDS_ALERT_WINDOW_HOURS
        count = await app_module.postgres_db_instance.count_rewards_since_hours(window_hours)
        return {
            "rewards_count_in_window": count,
            "window_hours": window_hours,
            "would_alert": count == 0,
            "note": "Alert fires when count is 0 after grace period. Use short env (e.g. REWARDS_ALERT_WINDOW_HOURS=0.001, REWARDS_ALERT_GRACE_SECONDS=10) to test quickly.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get rewards alert status: {str(e)}")


@router.post("/test/trigger-rewards-alert")
async def trigger_rewards_alert():
    """Run one rewards check and send alert if no rewards in window (for testing notifications)."""
    import backend.app as app_module

    if app_module.postgres_db_instance is None:
        raise HTTPException(status_code=503, detail="PostgreSQL not initialized")

    try:
        window_hours = app_module.REWARDS_ALERT_WINDOW_HOURS
        count = await app_module.postgres_db_instance.count_rewards_since_hours(window_hours)
        if count > 0:
            return {
                "message": "No alert sent: rewards exist in window",
                "rewards_count_in_window": count,
                "window_hours": window_hours,
            }
        import time
        subject = "Test/Manual: No Rewards Recorded"
        message = (
            f"No participant rewards in the last {window_hours} hours (triggered via /v1/test/trigger-rewards-alert).\n\n"
            f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
        )
        alert_sent = False
        if app_module.email_alert_instance and await app_module.email_alert_instance.send_alert(subject, message):
            alert_sent = True
        if app_module.webhook_alert_instance and await app_module.webhook_alert_instance.send_alert(subject, message):
            alert_sent = True
        return {
            "message": "Alert sent (no rewards in window)" if alert_sent else "Alert not sent (email/webhook not configured)",
            "rewards_count_in_window": 0,
            "window_hours": window_hours,
            "email_sent": app_module.email_alert_instance is not None and app_module.email_alert_instance.enabled,
            "webhook_sent": app_module.webhook_alert_instance is not None and app_module.webhook_alert_instance.enabled,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger rewards alert: {str(e)}")

