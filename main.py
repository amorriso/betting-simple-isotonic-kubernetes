# Filename: main.py
import os
import datetime
import time
import base64
import hashlib
import re
from typing import Dict, Any, List, Tuple

import django
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from loggingfancy import setup_logger

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ultimora.settings")
django.setup()

from django.utils import timezone
from django.db.models import OuterRef, Subquery

import competition.models as competition_models
import feed.models as feed_models
import cpmodels.simpleisotonic as simpleisotonic

logger = setup_logger(__name__, loglevel="INFO")

cleanup_map = {
    "Racing": "",
    "-": "",
    " ": "-",
}

code_map = {
    "Simple": "s",
    "IsotonicModel": "iso",
    "InversionModel": "im",
    "AutoDiff": "ad",
    "Betfair": "bf",
    "TAB": "tb",
    "Greyhound": "gr",
    "horse": "ho",
    "harness": "ha",
    "dog": "dg",
    "win": "wi",
    "place": "pl",
    "exacta": "ex",
    "quinella": "qu",
    "trifecta": "tr",
    "api": "",
}


def shorten(name: str) -> str:
    for old, new in cleanup_map.items():
        name = name.replace(old, new)

    for pattern, code in code_map.items():
        name = re.sub(re.escape(pattern), code, name, flags=re.IGNORECASE)

    name = name.lower().replace("--", "-").strip("-")
    name = re.sub(r"wi(pl|qu|ex|tr)", r"\1", name)

    bettype_codes = ("wi", "pl", "qu", "ex", "tr")
    for code in bettype_codes:
        while code + code in name:
            name = name.replace(code + code, code)

    return name


def build_job_string(
    feed_slug: str,
    im_map_id: int,
    race,
    max_length: int = 63,
    include_sj: bool = True,
) -> str:
    def _rfc1123_sanitize_name(name: str) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9-]", "x", name)
        name = name.strip("-")
        return name or "x"

    def _int_to_base32(n: int) -> str:
        if n < 0:
            raise ValueError("im_map_id must be non-negative")
        n_bytes = max(1, (n.bit_length() + 7) // 8)
        raw = n.to_bytes(n_bytes, byteorder="big", signed=False)
        return base64.b32encode(raw).decode("ascii").lower().rstrip("=")

    def _consonant_skeleton(s: str) -> str:
        if not s:
            return s
        vowels = set("aeiou")
        first = s[0]
        rest = "".join(ch for ch in s[1:] if ch not in vowels)
        return first + rest

    def _hash_token(token: str, out_len: int) -> str:
        if token == "":
            return token
        alphabet = "abcdefghijklmnopqrstuvwxyz234567"
        digest_size = max(16, (out_len * 5 + 7) // 8)
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=digest_size).digest()
        b32 = base64.b32encode(digest).decode("ascii").lower().rstrip("=")
        token = "".join(ch for ch in b32 if ch in alphabet)
        return token[:out_len]

    def _shorten_from_right(name: str, max_len: int, min_token_len: int = 10) -> str:
        parts = [part for part in name.split("-") if part]
        if not parts:
            return "x"

        if len(name) <= max_len:
            return name

        for collapsed_count in range(1, len(parts) + 1):
            prefix_parts = parts[:-collapsed_count]
            collapsed_suffix = "-".join(parts[-collapsed_count:])
            separator_len = 1 if prefix_parts else 0
            available_token_len = max_len - len("-".join(prefix_parts)) - separator_len

            if available_token_len < min_token_len:
                continue

            token = _hash_token(collapsed_suffix, available_token_len)
            candidate_parts = prefix_parts + [token]
            candidate = "-".join(candidate_parts).rstrip("-")
            if len(candidate) <= max_len:
                return candidate

        return _hash_token(name, max_len)

    feed_slug = _rfc1123_sanitize_name(str(feed_slug))
    start = f"{feed_slug}-{_int_to_base32(int(im_map_id))}"

    raw_venue = (race.venue.name or "").lower()
    venue_name = re.sub(r"[^a-z0-9]", "", raw_venue) or "venue"

    sj_dt = race.scheduled_jump
    sj = f"{sj_dt.day:02d}{sj_dt.hour:02d}{sj_dt.minute:02d}"

    def _compose(v: str) -> str:
        if include_sj:
            return f"{start}-{v}-{sj}"
        return f"{start}-{v}"

    job_str = _compose(venue_name)

    if len(job_str) > max_length:
        skel = _consonant_skeleton(venue_name) or "v"
        venue_name = skel
        job_str = _compose(venue_name)

    if len(job_str) > max_length:
        overhead = len(start) + 1
        if include_sj:
            overhead += 1 + len(sj)
        allowed_venue = max_length - overhead
        allowed_venue = max(1, allowed_venue)

        venue_name = venue_name[:allowed_venue].rstrip("-")
        job_str = _compose(venue_name)

    job_str = job_str.rstrip("-")

    if len(job_str) > max_length:
        job_str = _shorten_from_right(job_str, max_length)

    return _rfc1123_sanitize_name(job_str)


def is_job_terminal(job) -> bool:
    status = job.status
    if not status:
        return False

    completion_time = getattr(status, "completion_time", None)
    if completion_time is not None:
        return True

    conditions = status.conditions or []
    for condition in conditions:
        if condition.status == "True" and condition.type in ("Complete", "Failed"):
            return True

    succeeded = status.succeeded
    if succeeded is not None and succeeded >= 1:
        return True

    failed = status.failed
    if failed is not None and failed >= 1:
        return True

    return False


def count_active_jobs(
    batch_api: client.BatchV1Api,
    namespace: str,
    label_selector: str,
    limit: int = 500,
) -> int:
    total = 0
    _continue = None
    while True:
        jobs = batch_api.list_namespaced_job(
            namespace=namespace,
            label_selector=label_selector,
            limit=limit,
            _continue=_continue,
        )
        for job in jobs.items:
            if not is_job_terminal(job):
                total += 1

        _continue = jobs.metadata._continue
        if not _continue:
            break

    return total


try:
    CPU_COUNT = int(os.environ["CPU_COUNT"])
    logger.info(
        "%s Loaded CPU_COUNT=%s",
        simpleisotonic.build_simpleisotonic_log_ctx(),
        CPU_COUNT,
    )
except KeyError:
    logger.error(
        "%s Environment variable CPU_COUNT is not set; aborting pod startup.",
        simpleisotonic.build_simpleisotonic_log_ctx(),
    )
    raise

try:
    JOB_CPU_REQUEST = float(os.environ["JOB_CPU_REQUEST"])
    logger.info(
        "%s Loaded JOB_CPU_REQUEST=%s (cores per simple-isotonic-predict pod request)",
        simpleisotonic.build_simpleisotonic_log_ctx(),
        JOB_CPU_REQUEST,
    )
except KeyError:
    JOB_CPU_REQUEST = 0.05
    logger.info(
        "%s JOB_CPU_REQUEST not set; defaulting to %s",
        simpleisotonic.build_simpleisotonic_log_ctx(),
        JOB_CPU_REQUEST,
    )

SCHEDULER_TICK_SECONDS = int(os.getenv("SCHEDULER_TICK_SECONDS", "30"))
JOB_SPAWN_LEAD_SECONDS = int(os.getenv("JOB_SPAWN_LEAD_SECONDS", str(12 * 60)))


def _cpu_to_k8s_str(cpu: float) -> str:
    if cpu <= 0:
        raise ValueError("JOB_CPU_REQUEST must be positive")

    rounded = round(cpu)
    if abs(cpu - rounded) < 1e-9:
        return str(int(rounded))

    milli = int(round(cpu * 1000))
    return f"{milli}m"


def simple_isotonic_initial_fetch():
    today = timezone.now().date()
    spec_qs = feed_models.SimpleIsotonicModelSpecification.objects.filter(
        output_feed__trading_status="ACTIVE"
    )
    spec_count = spec_qs.count()

    logger.info(
        "%s Found %s SimpleIsotonicModelSpecification rows for initial_fetch()",
        simpleisotonic.build_simpleisotonic_log_ctx(),
        spec_count,
    )

    if spec_count == 0:
        return

    latest_instance_subquery = (
        feed_models.SimpleIsotonicModelInstance.objects.filter(
            specification=OuterRef("pk"),
            raceday__date__lt=today,
        )
        .order_by("-raceday__date")
        .values("pk")[:1]
    )

    spec_with_latest = spec_qs.annotate(
        latest_instance_id=Subquery(latest_instance_subquery)
    ).filter(latest_instance_id__isnull=False)

    latest_instance_ids = list(
        spec_with_latest.values_list("latest_instance_id", flat=True)
    )

    if not latest_instance_ids:
        logger.info(
            "%s No past SimpleIsotonicModelInstance rows found for any spec; nothing to initial_fetch().",
            simpleisotonic.build_simpleisotonic_log_ctx(),
        )
        return

    inst_list = (
        feed_models.SimpleIsotonicModelInstance.objects.filter(pk__in=latest_instance_ids)
        .select_related(
            "raceday",
            "specification",
            "specification__input_feed",
            "specification__output_feed",
            "specification__region_group",
        )
    )

    logger.info(
        "%s Found %s latest past SimpleIsotonicModelInstance rows for initial_fetch()",
        simpleisotonic.build_simpleisotonic_log_ctx(),
        inst_list.count(),
    )

    for inst in inst_list:
        logger.info(
            "%s Running initial_fetch for SimpleIsotonicModelInstance id=%s (spec_id=%s, raceday=%s)",
            simpleisotonic.build_simpleisotonic_log_ctx(instance=inst),
            inst.id,
            inst.specification.id,
            inst.raceday.date,
        )
        utils = simpleisotonic.SimpleIsotonicModelUtils(inst)
        utils.initial_fetch()


def find_due_instance_maps(
    window_start: datetime.datetime,
    window_end: datetime.datetime,
) -> List[Tuple[competition_models.Race, feed_models.SimpleIsotonicModelInstanceRawRaceMap]]:
    ctx = simpleisotonic.build_simpleisotonic_log_ctx()
    instance_maps = (
        feed_models.SimpleIsotonicModelInstanceRawRaceMap.objects.filter(
            rawracemap__race__scheduled_jump__gte=window_start,
            rawracemap__race__scheduled_jump__lte=window_end,
            instance__specification__output_feed__trading_status="ACTIVE",
        )
        .select_related(
            "instance",
            "instance__raceday",
            "instance__specification",
            "instance__specification__output_feed",
            "instance__specification__output_feed__oddstype",
            "rawracemap",
            "rawracemap__race",
            "rawracemap__race__venue",
        )
    )

    logger.info(
        "%s [simple-isotonic] Planning: %s instance maps between %s and %s",
        ctx,
        instance_maps.count(),
        window_start.isoformat(),
        window_end.isoformat(),
    )

    out: List[Tuple[competition_models.Race, feed_models.SimpleIsotonicModelInstanceRawRaceMap]] = []
    for im_map in instance_maps:
        out.append((im_map.rawracemap.race, im_map))

    out.sort(key=lambda item: item[0].scheduled_jump)
    return out


def spawn_simple_isotonic_job_for_race(
    batch_api: client.BatchV1Api,
    race,
    im_map: feed_models.SimpleIsotonicModelInstanceRawRaceMap,
    self_image: str,
):
    instance = im_map.instance
    spec = instance.specification
    output_feed = spec.output_feed
    bettype = output_feed.oddstype

    feed_string = f"{output_feed.name}-{bettype.name}"
    feed_slug = shorten(feed_string)

    job_name = build_job_string(feed_slug=feed_slug, im_map_id=im_map.id, race=race)

    ctx = simpleisotonic.build_simpleisotonic_log_ctx(race=race, instance=instance)
    cpu_str = _cpu_to_k8s_str(JOB_CPU_REQUEST)

    logger.info(
        "%s [simple-isotonic] Creating multi-rt Job %s for instance_map_id=%s race=%s",
        ctx,
        job_name,
        im_map.id,
        race.guid,
    )

    active_deadline_seconds = int(
        os.getenv("SIMPLE_ISOTONIC_JOB_ACTIVE_DEADLINE_SECONDS", "7200")
    )

    job_manifest: Dict[str, Any] = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": {
                "app": "simple-isotonic-predict",
                "race-guid": re.sub(
                    r"[^a-z0-9-]", "x", str(race.guid).lower()
                )[:63].strip("-") or "x",
            },
        },
        "spec": {
            "ttlSecondsAfterFinished": 600,
            "activeDeadlineSeconds": active_deadline_seconds,
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": {"app": "simple-isotonic-predict"}},
                "spec": {
                    "nodeSelector": {"compute": "true"},
                    "topologySpreadConstraints": [
                        {
                            "maxSkew": 1,
                            "topologyKey": "kubernetes.io/hostname",
                            "whenUnsatisfiable": "ScheduleAnyway",
                            "labelSelector": {
                                "matchLabels": {"app": "simple-isotonic-predict"}
                            },
                        }
                    ],
                    "containers": [
                        {
                            "name": "simple-isotonic-predict",
                            "image": self_image,
                            "env": [
                                {"name": "METHOD", "value": "simple_isotonic_predict_multi"},
                                {
                                    "name": "SIMPLE_ISOTONIC_MODEL_INSTANCE_MAP_ID",
                                    "value": str(im_map.id),
                                },
                            ],
                            "envFrom": [
                                {"configMapRef": {"name": "simple-isotonic-config"}},
                                {"secretRef": {"name": "database-credentials"}},
                            ],
                            "resources": {
                                "requests": {"cpu": cpu_str, "memory": "256Mi"},
                                "limits": {"cpu": "500m", "memory": "512Mi"},
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                },
            },
        },
    }

    try:
        batch_api.create_namespaced_job(namespace="default", body=job_manifest)
        logger.info("%s [simple-isotonic] Spawned Job %s", ctx, job_name)
    except ApiException as e:
        if e.status == 409:
            logger.info(
                "%s [simple-isotonic] Job %s already exists (409); skipping",
                ctx,
                job_name,
            )
            return
        logger.error(
            "%s [simple-isotonic] Failed to spawn Job %s: %s",
            ctx,
            job_name,
            e,
        )
    except Exception as e:
        logger.error(
            "%s [simple-isotonic] Failed to spawn Job %s: %s",
            ctx,
            job_name,
            e,
        )


def schedule_simple_isotonic_predictions_simple():
    ctx = simpleisotonic.build_simpleisotonic_log_ctx()

    try:
        config.load_incluster_config()
        logger.info("%s Loaded in-cluster Kubernetes configuration successfully.", ctx)
    except Exception as e:
        logger.error("%s Failed to load in-cluster config: %s", ctx, e)
        return

    self_image = os.getenv("SELF_IMAGE")
    logger.info("%s Using SELF_IMAGE=%s", ctx, self_image)

    batch_api = client.BatchV1Api()

    while True:
        tick_start = timezone.now()
        window_start = tick_start + datetime.timedelta(seconds=JOB_SPAWN_LEAD_SECONDS)
        window_end = window_start + datetime.timedelta(seconds=SCHEDULER_TICK_SECONDS)

        logger.info(
            "%s [simple-isotonic] Scheduler tick: now=%s spawn_window_start=%s spawn_window_end=%s",
            ctx,
            tick_start.isoformat(),
            window_start.isoformat(),
            window_end.isoformat(),
        )

        active_jobs = count_active_jobs(
            batch_api,
            namespace="default",
            label_selector="app=simple-isotonic-predict",
        )

        current_cpu = active_jobs * JOB_CPU_REQUEST
        available_cpu = float(CPU_COUNT) - current_cpu
        available_slots = int(available_cpu // JOB_CPU_REQUEST) if available_cpu > 0 else 0

        logger.info(
            "%s [simple-isotonic] Capacity: active_jobs=%s current_cpu=%.3f available_cpu=%.3f available_slots=%s (CPU_COUNT=%s, per_job=%s)",
            ctx,
            active_jobs,
            current_cpu,
            available_cpu,
            available_slots,
            CPU_COUNT,
            JOB_CPU_REQUEST,
        )

        if available_slots <= 0:
            logger.info(
                "%s [simple-isotonic] No capacity (available_cpu=%.3f, available_slots=%s); sleeping %ss",
                ctx,
                available_cpu,
                available_slots,
                SCHEDULER_TICK_SECONDS,
            )
            time.sleep(SCHEDULER_TICK_SECONDS)
            continue

        pairs = find_due_instance_maps(window_start=window_start, window_end=window_end)

        if not pairs:
            logger.info(
                "%s [simple-isotonic] No (race, instance_map) pairs found; sleeping %ss",
                ctx,
                SCHEDULER_TICK_SECONDS,
            )
            time.sleep(SCHEDULER_TICK_SECONDS)
            continue

        due = len(pairs)
        spawned = 0
        for race, im_map in pairs:
            if spawned >= available_slots:
                break
            spawn_simple_isotonic_job_for_race(
                batch_api, race=race, im_map=im_map, self_image=self_image
            )
            spawned += 1

        skipped_due_to_capacity = max(due - spawned, 0)

        logger.info(
            "%s [simple-isotonic] Tick done: available_cpu=%.3f available_slots=%s due=%s spawned=%s skipped_due_to_capacity=%s sleeping=%ss",
            ctx,
            available_cpu,
            available_slots,
            due,
            spawned,
            skipped_due_to_capacity,
            SCHEDULER_TICK_SECONDS,
        )
        time.sleep(SCHEDULER_TICK_SECONDS)


def run_simple_isotonic_predict_multi():
    im_map_id = os.getenv("SIMPLE_ISOTONIC_MODEL_INSTANCE_MAP_ID")
    if not im_map_id:
        logger.error(
            "%s SIMPLE_ISOTONIC_MODEL_INSTANCE_MAP_ID not set",
            simpleisotonic.build_simpleisotonic_log_ctx(),
        )
        return

    try:
        im_map = feed_models.SimpleIsotonicModelInstanceRawRaceMap.objects.get(pk=im_map_id)
    except feed_models.SimpleIsotonicModelInstanceRawRaceMap.DoesNotExist:
        logger.error(
            "%s No SimpleIsotonicModelInstanceRawRaceMap with id=%s",
            simpleisotonic.build_simpleisotonic_log_ctx(),
            im_map_id,
        )
        return

    utils = simpleisotonic.SimpleIsotonicPredictionModelUtils(im_map)
    race = utils.race
    sj = race.scheduled_jump

    ctx = simpleisotonic.build_simpleisotonic_log_ctx(race=race, instance=utils.instance)

    available_rts = utils._available_rts
    if not available_rts:
        logger.warning("%s No available_rts; exiting", ctx)
        return

    rts = sorted(int(rt) for rt in available_rts)

    logger.info(
        "%s [simple-isotonic] Starting multi-rt job: instance_map_id=%s race=%s scheduled_jump=%s rts=%s",
        ctx,
        im_map_id,
        race.guid,
        sj,
        rts,
    )

    for rt in rts:
        fire_time = sj + datetime.timedelta(seconds=rt)
        target_time = fire_time - datetime.timedelta(seconds=6)

        now = timezone.now()
        sleep_for = (target_time - now).total_seconds()

        if sleep_for > 0:
            logger.info(
                "%s [simple-isotonic] Sleeping %.1fs until target_time=%s (rt=%s fire_time=%s)",
                ctx,
                sleep_for,
                target_time.isoformat(),
                rt,
                fire_time.isoformat(),
            )
            time.sleep(sleep_for)
        else:
            logger.warning(
                "%s [simple-isotonic] target_time already passed (target=%s now=%s) for rt=%s; skipping",
                ctx,
                target_time.isoformat(),
                now.isoformat(),
                rt,
            )
            continue

        logger.info(
            "%s [simple-isotonic] Running predict() for rt=%s (target_time=%s)",
            ctx,
            rt,
            target_time.isoformat(),
        )
        try:
            utils.predict()
            logger.info("%s [simple-isotonic] predict() complete for rt=%s", ctx, rt)
        except Exception as e:
            logger.error(
                "%s [simple-isotonic] predict() failed for rt=%s: %s",
                ctx,
                rt,
                e,
                exc_info=True,
            )

    logger.info("%s [simple-isotonic] All rts processed; job complete", ctx)


def debug_print_simple_isotonic_plan_simple():
    now = timezone.now()
    window_start = now + datetime.timedelta(seconds=JOB_SPAWN_LEAD_SECONDS)
    window_end = window_start + datetime.timedelta(seconds=SCHEDULER_TICK_SECONDS)

    pairs = find_due_instance_maps(window_start=window_start, window_end=window_end)

    print(
        f"[simple-isotonic][plan] now={now.isoformat()} "
        f"spawn_window_start={window_start.isoformat()} "
        f"spawn_window_end={window_end.isoformat()} pairs={len(pairs)}"
    )
    for race, im_map in pairs:
        print(
            f"race={race.guid} sj={race.scheduled_jump.isoformat()} "
            f"venue={race.venue.name} instance_map_id={im_map.id}"
        )


if __name__ == "__main__":
    method = os.getenv("METHOD")
    logger.info(
        "%s [simple-isotonic] Entrypoint with METHOD=%s",
        simpleisotonic.build_simpleisotonic_log_ctx(),
        method,
    )

    if method == "simple_isotonic_initial_fetch":
        simple_isotonic_initial_fetch()
    elif method == "simple_isotonic_schedule":
        schedule_simple_isotonic_predictions_simple()
    elif method == "simple_isotonic_predict_multi":
        run_simple_isotonic_predict_multi()
    elif method == "simple_isotonic_plan":
        debug_print_simple_isotonic_plan_simple()
    else:
        logger.error(
            "%s [simple-isotonic] Unknown or unset METHOD=%r",
            simpleisotonic.build_simpleisotonic_log_ctx(),
            method,
        )
