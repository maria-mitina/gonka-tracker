import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.router import router, set_inference_service
from backend.client import GonkaClient
from backend.database import CacheDB
from backend.postgres_db import PostgresDB
from backend.service import InferenceService
from backend.email_alert import EmailAlert
from backend.webhook_alert import WebhookAlert

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:     %(message)s'
)
logger = logging.getLogger(__name__)

POLL_CURRENT_EPOCH_INTERVAL = int(os.getenv("POLL_CURRENT_EPOCH_INTERVAL", "30"))
POLL_JAIL_STATUS_INTERVAL = int(os.getenv("POLL_JAIL_STATUS_INTERVAL", "120"))
POLL_NODE_HEALTH_INTERVAL = int(os.getenv("POLL_NODE_HEALTH_INTERVAL", "60"))
POLL_REWARDS_INTERVAL = int(os.getenv("POLL_REWARDS_INTERVAL", "60"))
POLL_WARM_KEYS_INTERVAL = int(os.getenv("POLL_WARM_KEYS_INTERVAL", "300"))
POLL_WARM_KEYS_BATCH_SIZE = int(os.getenv("POLL_WARM_KEYS_BATCH_SIZE", "10"))
POLL_HARDWARE_NODES_INTERVAL = int(os.getenv("POLL_HARDWARE_NODES_INTERVAL", "600"))
POLL_HARDWARE_NODES_BATCH_SIZE = int(os.getenv("POLL_HARDWARE_NODES_BATCH_SIZE", "10"))
POLL_EPOCH_TOTAL_REWARDS_INTERVAL = int(os.getenv("POLL_EPOCH_TOTAL_REWARDS_INTERVAL", "600"))
POLL_PARTICIPANT_INFERENCES_INTERVAL = int(os.getenv("POLL_PARTICIPANT_INFERENCES_INTERVAL", "1200"))
POLL_MODELS_API_INTERVAL = int(os.getenv("POLL_MODELS_API_INTERVAL", "300"))
POLL_TIMELINE_INTERVAL = int(os.getenv("POLL_TIMELINE_INTERVAL", "30"))
POLL_CONFIRMATION_DATA_INTERVAL = int(os.getenv("POLL_CONFIRMATION_DATA_INTERVAL", "120"))
BLOCK_HEIGHT_CHECK_INTERVAL = int(os.getenv("BLOCK_HEIGHT_CHECK_INTERVAL", "30"))
BLOCK_HEIGHT_ALERT_THRESHOLD = int(os.getenv("BLOCK_HEIGHT_ALERT_THRESHOLD", "120"))
BLOCK_HEIGHT_REMINDER_INTERVAL = int(os.getenv("BLOCK_HEIGHT_REMINDER_INTERVAL", "300"))
REWARDS_ALERT_CHECK_INTERVAL = int(os.getenv("REWARDS_ALERT_CHECK_INTERVAL", "600"))
REWARDS_ALERT_WINDOW_HOURS = float(os.getenv("REWARDS_ALERT_WINDOW_HOURS", "24"))
REWARDS_ALERT_REMINDER_INTERVAL = int(os.getenv("REWARDS_ALERT_REMINDER_INTERVAL", "3600"))
REWARDS_ALERT_GRACE_SECONDS = int(os.getenv("REWARDS_ALERT_GRACE_SECONDS", "1800"))
EPOCH_FETCH_ALERT_CONSECUTIVE_THRESHOLD = int(os.getenv("EPOCH_FETCH_ALERT_CONSECUTIVE_THRESHOLD", "3"))
EPOCH_FETCH_ALERT_REMINDER_INTERVAL = int(os.getenv("EPOCH_FETCH_ALERT_REMINDER_INTERVAL", "3600"))

background_task = None
jail_polling_task = None
health_polling_task = None
rewards_polling_task = None
warm_keys_polling_task = None
hardware_nodes_polling_task = None
epoch_total_rewards_polling_task = None
participant_inferences_polling_task = None
models_api_polling_task = None
timeline_polling_task = None
confirmation_polling_task = None
block_height_monitoring_task = None
rewards_monitoring_task = None
inference_service_instance = None
email_alert_instance = None
webhook_alert_instance = None
block_height_test_mode = False
block_height_test_fixed_height = None
consecutive_epoch_fetch_failures = 0
last_epoch_fetch_alert_time = None


async def poll_current_epoch():
    import time
    global consecutive_epoch_fetch_failures, last_epoch_fetch_alert_time, email_alert_instance, webhook_alert_instance

    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.get_current_epoch_stats(reload=True)
                consecutive_epoch_fetch_failures = 0
                last_epoch_fetch_alert_time = None
                logger.info("Background polling: fetched current epoch stats")
        except Exception as e:
            logger.error("Background polling error: %s", e)
            consecutive_epoch_fetch_failures += 1
            if consecutive_epoch_fetch_failures >= EPOCH_FETCH_ALERT_CONSECUTIVE_THRESHOLD:
                current_time = time.time()
                time_since_last_alert = current_time - last_epoch_fetch_alert_time if last_epoch_fetch_alert_time else float("inf")
                should_send = last_epoch_fetch_alert_time is None or time_since_last_alert >= EPOCH_FETCH_ALERT_REMINDER_INTERVAL
                if should_send:
                    is_reminder = last_epoch_fetch_alert_time is not None
                    alert_type = "Reminder: " if is_reminder else ""
                    subject = f"{alert_type}Alert: Chain/API unreachable"
                    message = (
                        f"get_current_epoch_stats() has failed {consecutive_epoch_fetch_failures} times in a row.\n\n"
                        f"Last error: {e}\n\n"
                        f"Threshold: {EPOCH_FETCH_ALERT_CONSECUTIVE_THRESHOLD} consecutive failures\n"
                        f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
                    )
                    alert_sent = False
                    if email_alert_instance and await email_alert_instance.send_alert(subject, message):
                        alert_sent = True
                    if webhook_alert_instance and await webhook_alert_instance.send_alert(subject, message):
                        alert_sent = True
                    if alert_sent:
                        last_epoch_fetch_alert_time = current_time
                        logger.warning("Alert sent: Chain/API unreachable (%s consecutive failures)", consecutive_epoch_fetch_failures)
        await asyncio.sleep(POLL_CURRENT_EPOCH_INTERVAL)


async def poll_jail_status():
    await asyncio.sleep(10)
    
    while True:
        try:
            if inference_service_instance:
                epoch_data = await inference_service_instance.client.get_current_epoch_participants()
                epoch_id = epoch_data["active_participants"]["epoch_group_id"]
                height = await inference_service_instance.client.get_latest_height()
                active_participants = epoch_data["active_participants"]["participants"]
                
                await inference_service_instance.fetch_and_cache_jail_statuses(
                    epoch_id, height, active_participants
                )
                logger.info("Background polling: fetched jail statuses")
        except Exception as e:
            logger.error(f"Jail polling error: {e}")
        
        await asyncio.sleep(POLL_JAIL_STATUS_INTERVAL)


async def poll_node_health():
    await asyncio.sleep(5)
    
    while True:
        try:
            if inference_service_instance:
                epoch_data = await inference_service_instance.client.get_current_epoch_participants()
                active_participants = epoch_data["active_participants"]["participants"]
                
                await inference_service_instance.fetch_and_cache_node_health(active_participants)
                logger.info("Background polling: fetched node health")
        except Exception as e:
            logger.error(f"Node health polling error: {e}")
        
        await asyncio.sleep(POLL_NODE_HEALTH_INTERVAL)


async def poll_rewards():
    await asyncio.sleep(15)
    
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.poll_participant_rewards()
        except Exception as e:
            logger.error(f"Rewards polling error: {e}")
        
        await asyncio.sleep(POLL_REWARDS_INTERVAL)


async def poll_warm_keys():
    await asyncio.sleep(20)
    
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.poll_warm_keys(batch_size=POLL_WARM_KEYS_BATCH_SIZE)
        except Exception as e:
            logger.error(f"Warm keys polling error: {e}")
        
        await asyncio.sleep(POLL_WARM_KEYS_INTERVAL)


async def poll_hardware_nodes():
    await asyncio.sleep(25)
    
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.poll_hardware_nodes(batch_size=POLL_HARDWARE_NODES_BATCH_SIZE)
        except Exception as e:
            logger.error(f"Hardware nodes polling error: {e}")
        
        await asyncio.sleep(POLL_HARDWARE_NODES_INTERVAL)


async def poll_epoch_total_rewards():
    await asyncio.sleep(30)
    
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.poll_epoch_total_rewards()
        except Exception as e:
            logger.error(f"Epoch total rewards polling error: {e}")
        
        await asyncio.sleep(POLL_EPOCH_TOTAL_REWARDS_INTERVAL)


async def poll_participant_inferences():
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.poll_participant_inferences()
        except Exception as e:
            logger.error(f"Participant inferences polling error: {e}")
        
        await asyncio.sleep(POLL_PARTICIPANT_INFERENCES_INTERVAL)


async def poll_models_api():
    await asyncio.sleep(35)
    
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.poll_models_api_cache()
        except Exception as e:
            logger.error(f"Models API polling error: {e}")
        
        await asyncio.sleep(POLL_MODELS_API_INTERVAL)


async def poll_timeline():
    await asyncio.sleep(40)
    
    while True:
        try:
            if inference_service_instance:
                await inference_service_instance.get_timeline()
                logger.info("Background polling: fetched timeline data")
        except Exception as e:
            logger.error(f"Timeline polling error: {e}")
        
        await asyncio.sleep(POLL_TIMELINE_INTERVAL)


async def poll_confirmation_data():
    await asyncio.sleep(5)
    
    while True:
        try:
            if inference_service_instance:
                epoch_data = await inference_service_instance.client.get_current_epoch_participants()
                epoch_id = epoch_data["active_participants"]["epoch_group_id"]
                height = await inference_service_instance.client.get_latest_height()
                active_participants = epoch_data["active_participants"]["participants"]
                
                await inference_service_instance.fetch_and_cache_confirmation_data(
                    epoch_id, height, active_participants
                )
                logger.info("Background polling: fetched confirmation data")
        except Exception as e:
            logger.error(f"Confirmation data polling error: {e}")
        
        await asyncio.sleep(POLL_CONFIRMATION_DATA_INTERVAL)


async def monitor_block_height():
    import time

    await asyncio.sleep(10)

    last_height = None
    last_height_time = None
    last_alert_time = None

    while True:
        try:
            if inference_service_instance:
                if block_height_test_mode and block_height_test_fixed_height is not None:
                    current_height = block_height_test_fixed_height
                    latest_block_time = None
                    logger.debug(f"Test mode: Using fixed height {current_height}")
                else:
                    sync_info = await inference_service_instance.client.get_status_sync_info()
                    current_height = sync_info["latest_block_height"]
                    latest_block_time = sync_info.get("latest_block_time")
                current_time = time.time()

                if last_height is not None:
                    if current_height > last_height:
                        old_height = last_height
                        last_height = current_height
                        last_height_time = current_time
                        last_alert_time = None
                        logger.info(f"Block height increased: {old_height} -> {current_height}")
                    else:
                        if latest_block_time is not None:
                            time_since_last_block = current_time - latest_block_time
                        else:
                            time_since_last_block = current_time - last_height_time if last_height_time else 0
                        time_since_last_alert = current_time - last_alert_time if last_alert_time else float("inf")

                        should_send_alert = False
                        is_reminder = False

                        if time_since_last_block >= BLOCK_HEIGHT_ALERT_THRESHOLD:
                            if last_alert_time is None:
                                should_send_alert = True
                                logger.info("Time since last block %ss (height %s) - sending initial alert", int(time_since_last_block), current_height)
                            elif time_since_last_alert >= BLOCK_HEIGHT_REMINDER_INTERVAL:
                                should_send_alert = True
                                is_reminder = True
                                logger.info("Time since last block %ss (height %s) - sending reminder alert", int(time_since_last_block), current_height)

                        if should_send_alert:
                            alert_type = "Reminder: " if is_reminder else ""
                            subject = f"{alert_type}Alert: Time Since Last Block"
                            message = (
                                f"Time since last block: {int(time_since_last_block)} seconds.\n\n"
                                f"Current block height: {current_height}\n"
                                f"Threshold: {BLOCK_HEIGHT_ALERT_THRESHOLD} seconds\n"
                                f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
                            )

                            alert_sent_any = False
                            if email_alert_instance:
                                if await email_alert_instance.send_alert(subject, message):
                                    alert_sent_any = True

                            if webhook_alert_instance:
                                if await webhook_alert_instance.send_alert(subject, message):
                                    alert_sent_any = True

                            if alert_sent_any:
                                last_alert_time = current_time
                                logger.warning("Alert sent: Time since last block %s seconds", int(time_since_last_block))
                            else:
                                logger.warning("No alert channels configured: Time since last block %ss at height %s", int(time_since_last_block), current_height)
                        else:
                            logger.debug("Time since last block %ss at height %s (threshold: %ss)", int(time_since_last_block), current_height, BLOCK_HEIGHT_ALERT_THRESHOLD)
                else:
                    last_height = current_height
                    last_height_time = current_time
                    logger.info("Initial block height: %s", current_height)

        except Exception as e:
            logger.error("Block height monitoring error: %s", e)

        await asyncio.sleep(BLOCK_HEIGHT_CHECK_INTERVAL)


async def monitor_rewards():
    import time
    global postgres_db_instance, email_alert_instance, webhook_alert_instance
    await asyncio.sleep(REWARDS_ALERT_GRACE_SECONDS)
    last_alert_time = None
    startup_time = time.time()
    while True:
        try:
            if postgres_db_instance and (time.time() - startup_time) >= REWARDS_ALERT_GRACE_SECONDS:
                count = await postgres_db_instance.count_rewards_since_hours(REWARDS_ALERT_WINDOW_HOURS)
                current_time = time.time()
                time_since_last_alert = current_time - last_alert_time if last_alert_time else float("inf")
                if count == 0:
                    should_send = False
                    is_reminder = False
                    if last_alert_time is None:
                        should_send = True
                        logger.info("No reward records in the last %s hours - sending initial alert", REWARDS_ALERT_WINDOW_HOURS)
                    elif time_since_last_alert >= REWARDS_ALERT_REMINDER_INTERVAL:
                        should_send = True
                        is_reminder = True
                        logger.info("No reward records in the last %s hours - sending reminder alert", REWARDS_ALERT_WINDOW_HOURS)
                    if should_send:
                        alert_type = "Reminder: " if is_reminder else ""
                        subject = f"{alert_type}Alert: No Rewards Recorded"
                        message = (
                            f"No participant rewards have been recorded in the last {REWARDS_ALERT_WINDOW_HOURS} hours.\n\n"
                            f"Check rewards polling and chain API.\n"
                            f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
                        )
                        alert_sent = False
                        if email_alert_instance and await email_alert_instance.send_alert(subject, message):
                            alert_sent = True
                        if webhook_alert_instance and await webhook_alert_instance.send_alert(subject, message):
                            alert_sent = True
                        if alert_sent:
                            last_alert_time = current_time
                            logger.warning("Alert sent: No rewards in the last %s hours", REWARDS_ALERT_WINDOW_HOURS)
                else:
                    last_alert_time = None
        except Exception as e:
            logger.error("Rewards monitoring error: %s", e)
        await asyncio.sleep(REWARDS_ALERT_CHECK_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_task, jail_polling_task, health_polling_task, rewards_polling_task, warm_keys_polling_task, hardware_nodes_polling_task, epoch_total_rewards_polling_task, participant_inferences_polling_task, models_api_polling_task, timeline_polling_task, confirmation_polling_task, block_height_monitoring_task, rewards_monitoring_task, inference_service_instance, email_alert_instance, webhook_alert_instance, postgres_db_instance
    
    inference_urls = os.getenv("INFERENCE_URLS", "http://node2.gonka.ai:8000").split(",")
    inference_urls = [url.strip() for url in inference_urls]
    
    db_path = os.getenv("CACHE_DB_PATH", "cache.db")
    
    logger.info(f"Initializing with URLs: {inference_urls}")
    logger.info(f"Database path: {db_path}")
    logger.info(f"Polling intervals (s): epoch={POLL_CURRENT_EPOCH_INTERVAL}, jail={POLL_JAIL_STATUS_INTERVAL}, health={POLL_NODE_HEALTH_INTERVAL}, rewards={POLL_REWARDS_INTERVAL}")
    logger.info(f"Polling intervals (s): warm_keys={POLL_WARM_KEYS_INTERVAL}, hardware_nodes={POLL_HARDWARE_NODES_INTERVAL}, total_rewards={POLL_EPOCH_TOTAL_REWARDS_INTERVAL}, inferences={POLL_PARTICIPANT_INFERENCES_INTERVAL}, models_api={POLL_MODELS_API_INTERVAL}, timeline={POLL_TIMELINE_INTERVAL}, confirmation_data={POLL_CONFIRMATION_DATA_INTERVAL}, block_height={BLOCK_HEIGHT_CHECK_INTERVAL}")
    logger.info(f"Polling batch sizes: warm_keys={POLL_WARM_KEYS_BATCH_SIZE}, hardware_nodes={POLL_HARDWARE_NODES_BATCH_SIZE}")
    logger.info(f"Block height alert threshold: {BLOCK_HEIGHT_ALERT_THRESHOLD}s, reminder interval: {BLOCK_HEIGHT_REMINDER_INTERVAL}s")
    logger.info(f"Rewards alert: check every {REWARDS_ALERT_CHECK_INTERVAL}s, window {REWARDS_ALERT_WINDOW_HOURS}h, reminder {REWARDS_ALERT_REMINDER_INTERVAL}s, grace {REWARDS_ALERT_GRACE_SECONDS}s")
    logger.info(f"Epoch fetch alert: threshold {EPOCH_FETCH_ALERT_CONSECUTIVE_THRESHOLD} consecutive failures, reminder every {EPOCH_FETCH_ALERT_REMINDER_INTERVAL}s")

    cache_db = CacheDB(db_path)
    await cache_db.initialize()
    
    postgres_db_instance = None
    try:
        postgres_db_instance = PostgresDB()
        await postgres_db_instance.initialize()
        logger.info("PostgreSQL database initialized for unified storage")
    except Exception as e:
        logger.warning(f"Failed to initialize PostgreSQL (metrics will not be written): {e}")
        logger.warning("Continuing with SQLite cache only. Check PostgreSQL configuration.")
    
    client = GonkaClient(base_urls=inference_urls)
    inference_service_instance = InferenceService(client=client, cache_db=cache_db, postgres_db=postgres_db_instance)
    email_alert_instance = EmailAlert()
    webhook_alert_instance = WebhookAlert()
    
    set_inference_service(inference_service_instance)
    
    background_task = asyncio.create_task(poll_current_epoch())
    jail_polling_task = asyncio.create_task(poll_jail_status())
    health_polling_task = asyncio.create_task(poll_node_health())
    rewards_polling_task = asyncio.create_task(poll_rewards())
    warm_keys_polling_task = asyncio.create_task(poll_warm_keys())
    hardware_nodes_polling_task = asyncio.create_task(poll_hardware_nodes())
    epoch_total_rewards_polling_task = asyncio.create_task(poll_epoch_total_rewards())
    participant_inferences_polling_task = asyncio.create_task(poll_participant_inferences())
    models_api_polling_task = asyncio.create_task(poll_models_api())
    timeline_polling_task = asyncio.create_task(poll_timeline())
    confirmation_polling_task = asyncio.create_task(poll_confirmation_data())
    block_height_monitoring_task = asyncio.create_task(monitor_block_height())
    rewards_monitoring_task = asyncio.create_task(monitor_rewards())
    logger.info("Background polling tasks started")
    
    yield
    
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            logger.info("Background polling task cancelled")
    
    if jail_polling_task:
        jail_polling_task.cancel()
        try:
            await jail_polling_task
        except asyncio.CancelledError:
            logger.info("Jail polling task cancelled")
    
    if health_polling_task:
        health_polling_task.cancel()
        try:
            await health_polling_task
        except asyncio.CancelledError:
            logger.info("Health polling task cancelled")
    
    if rewards_polling_task:
        rewards_polling_task.cancel()
        try:
            await rewards_polling_task
        except asyncio.CancelledError:
            logger.info("Rewards polling task cancelled")
    
    if warm_keys_polling_task:
        warm_keys_polling_task.cancel()
        try:
            await warm_keys_polling_task
        except asyncio.CancelledError:
            logger.info("Warm keys polling task cancelled")
    
    if hardware_nodes_polling_task:
        hardware_nodes_polling_task.cancel()
        try:
            await hardware_nodes_polling_task
        except asyncio.CancelledError:
            logger.info("Hardware nodes polling task cancelled")
    
    if epoch_total_rewards_polling_task:
        epoch_total_rewards_polling_task.cancel()
        try:
            await epoch_total_rewards_polling_task
        except asyncio.CancelledError:
            logger.info("Epoch total rewards polling task cancelled")
    
    if participant_inferences_polling_task:
        participant_inferences_polling_task.cancel()
        try:
            await participant_inferences_polling_task
        except asyncio.CancelledError:
            logger.info("Participant inferences polling task cancelled")
    
    if models_api_polling_task:
        models_api_polling_task.cancel()
        try:
            await models_api_polling_task
        except asyncio.CancelledError:
            logger.info("Models API polling task cancelled")
    
    if timeline_polling_task:
        timeline_polling_task.cancel()
        try:
            await timeline_polling_task
        except asyncio.CancelledError:
            logger.info("Timeline polling task cancelled")
    
    if confirmation_polling_task:
        confirmation_polling_task.cancel()
        try:
            await confirmation_polling_task
        except asyncio.CancelledError:
            logger.info("Confirmation polling task cancelled")
    
    if block_height_monitoring_task:
        block_height_monitoring_task.cancel()
        try:
            await block_height_monitoring_task
        except asyncio.CancelledError:
            logger.info("Block height monitoring task cancelled")

    if rewards_monitoring_task:
        rewards_monitoring_task.cancel()
        try:
            await rewards_monitoring_task
        except asyncio.CancelledError:
            logger.info("Rewards monitoring task cancelled")

    if postgres_db_instance:
        await postgres_db_instance.close()
        logger.info("PostgreSQL connection closed")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

