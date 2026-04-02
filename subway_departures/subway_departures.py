from plugins.base_plugin.base_plugin import BasePlugin
from pymta import SubwayFeed
from datetime import datetime
import aiohttp
import asyncio
import certifi
import csv
import io
import logging
import json
import os
import ssl
import pytz

logger = logging.getLogger(__name__)

MTA_STATIONS_CSV_URL = (
    "https://data.ny.gov/api/views/39hk-dx4f"
    "/rows.csv?accessType=DOWNLOAD"
)

LINE_TO_FEED = {
    "1": "1", "2": "1", "3": "1",
    "4": "1", "5": "1", "6": "1",
    "7": "1",
    "GS": "1",
    "A": "A", "C": "A", "E": "A", "H": "A",
    "N": "N", "Q": "N", "R": "N", "W": "N",
    "B": "B", "D": "B", "F": "B", "M": "B", "FS": "B",
    "L": "L",
    "SIR": "SI",
    "G": "G",
    "J": "J", "Z": "J",
}

# Maps display line names to the route_id used in the GTFS-RT feed.
# Most lines match, but a few differ.
LINE_TO_GTFS_ROUTE = {
    "SIR": "SI",
}

# Express variants → base line for display purposes.
# The GTFS-RT feed uses 7X/6X for express service; we show them
# as their base line number but with a diamond-shaped roundel.
EXPRESS_TO_BASE = {
    "7X": "7",
    "6X": "6",
}

# Shuttle internal IDs → display name.  Users see "S" for all shuttles.
SHUTTLE_DISPLAY = {"GS": "S", "FS": "S", "H": "S"}

# Reverse mapping: GTFS route_id back to display name
GTFS_ROUTE_TO_LINE = {v: k for k, v in LINE_TO_GTFS_ROUTE.items()}

# The MTA stations CSV lists all three shuttles as "S", but the
# GTFS-RT feeds use distinct route_ids.  We disambiguate by stop_id
# when building the station index.
SHUTTLE_STOPS = {
    "GS": {"901", "902"},
    "FS": {"D26", "S01", "S03", "S04"},
    "H":  {"H04", "H12", "H13", "H14", "H15", "H19"},
}

LINE_COLORS = {
    "1": "#EE352E", "2": "#EE352E", "3": "#EE352E",
    "4": "#00933C", "5": "#00933C", "6": "#00933C",
    "7": "#B933AD",
    "A": "#0039A6", "C": "#0039A6", "E": "#0039A6",
    "B": "#FF6319", "D": "#FF6319", "F": "#FF6319",
    "M": "#FF6319",
    "G": "#6CBE45",
    "J": "#996633", "Z": "#996633",
    "L": "#A7A9AC",
    "N": "#FCCC0A", "Q": "#FCCC0A", "R": "#FCCC0A",
    "W": "#FCCC0A",
    "GS": "#808183", "FS": "#808183", "H": "#808183",
    "SIR": "#0039A6",
}

CACHE_TTL_HOURS = 168  # 7 days — station data rarely changes
CACHE_FILE = os.path.join(
    os.path.dirname(__file__), "stations_cache.json"
)


def _create_ssl_context():
    """Create an SSL context using certifi's CA bundle."""
    return ssl.create_default_context(cafile=certifi.where())


def _create_ssl_session():
    """Create an aiohttp session with proper SSL cert verification."""
    connector = aiohttp.TCPConnector(ssl=_create_ssl_context())
    return aiohttp.ClientSession(connector=connector)


def _load_cached_stations():
    """Load station index from cache if fresh enough."""
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        age_hours = (
            (datetime.now().timestamp()
             - os.path.getmtime(CACHE_FILE)) / 3600
        )
        if age_hours > CACHE_TTL_HOURS:
            return None
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        if data:
            logger.info("Loaded station index from cache")
            return data
    except Exception as e:
        logger.warning(f"Could not read station cache: {e}")
    return None


def _save_stations_cache(stations):
    """Write station index to cache file."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(stations, f)
        logger.info("Saved station index to cache")
    except Exception as e:
        logger.warning(f"Could not write station cache: {e}")


def _fetch_station_index():
    """Download the MTA stations CSV and build a station index.

    Groups stations by Complex ID so physically connected
    stations (e.g. 59 St-Columbus Circle with 1/A/B/C/D)
    appear as a single entry, while stations that share a
    name but are separate (e.g. the various 125 St stations)
    remain distinct.

    Returns a dict keyed by complex_id:
        {
            "614": {
                "name": "59 St-Columbus Circle",
                "stop_ids": ["A24", "125"],
                "lines": ["1", "A", "B", "C", "D"],
                "north_label": "Uptown",
                "south_label": "Downtown",
            },
            ...
        }
    """
    import requests
    resp = requests.get(
        MTA_STATIONS_CSV_URL,
        timeout=30,
        verify=certifi.where(),
    )
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    complexes = {}

    for row in reader:
        cid = row["Complex ID"]
        gtfs_id = row["GTFS Stop ID"]
        name = row["Stop Name"]
        routes_str = row.get("Daytime Routes", "")
        north = row.get("North Direction Label", "Uptown")
        south = row.get("South Direction Label", "Downtown")

        routes = [
            r.strip() for r in routes_str.split()
            if r.strip()
        ]

        # The CSV uses "S" for all three shuttles.  Disambiguate
        # into GS / FS / H based on the stop_id.
        resolved_routes = []
        for r in routes:
            if r == "S":
                for shuttle_line, stop_set in SHUTTLE_STOPS.items():
                    if gtfs_id in stop_set:
                        resolved_routes.append(shuttle_line)
                        break
                else:
                    # Unknown shuttle stop — keep generic S
                    resolved_routes.append(r)
            else:
                resolved_routes.append(r)
        routes = resolved_routes

        if cid not in complexes:
            complexes[cid] = {
                "name": name,
                "stop_ids": [],
                "lines": [],
                "north_label": north or "Uptown",
                "south_label": south or "Downtown",
            }

        if gtfs_id not in complexes[cid]["stop_ids"]:
            complexes[cid]["stop_ids"].append(gtfs_id)

        for route in routes:
            if route not in complexes[cid]["lines"]:
                complexes[cid]["lines"].append(route)

        # Use the longest name in the complex
        if len(name) > len(complexes[cid]["name"]):
            complexes[cid]["name"] = name

    # Sort lines within each complex
    for info in complexes.values():
        info["lines"].sort(
            key=lambda x: (not x[0].isdigit(), x)
        )

    return complexes


def _get_station_index():
    """Return station index, using cache when available."""
    stations = _load_cached_stations()
    if stations:
        return stations
    logger.info("Building station index from MTA data...")
    stations = _fetch_station_index()
    if stations:
        _save_stations_cache(stations)
    return stations


class SubwayDepartures(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['style_settings'] = True

        try:
            stations = _get_station_index()
        except Exception as e:
            logger.error(f"Failed to preload station index: {e}")
            stations = {}

        template_params['stations_json'] = json.dumps(stations)
        # Build a display-friendly line_colors map that includes "S"
        display_colors = dict(LINE_COLORS)
        display_colors["S"] = LINE_COLORS.get("GS", "#808183")
        template_params['line_colors'] = display_colors
        return template_params

    def generate_image(self, settings, device_config):
        watches_json = settings.get('watches', '[]')
        try:
            watches = json.loads(watches_json)
        except (json.JSONDecodeError, TypeError):
            watches = []

        if not watches:
            raise RuntimeError(
                "Add at least one station to your board."
            )

        max_arrivals = int(
            settings.get('max_arrivals', 8)
        )
        min_minutes = int(
            settings.get('min_minutes', 0)
        )
        timezone = device_config.get_config(
            "timezone", default="America/New_York"
        )
        tz = pytz.timezone(timezone)
        now = datetime.now(tz)

        # Fetch departures for each watch
        station_groups = []
        all_fetch_args = []
        for watch in watches:
            stop_ids_list = watch.get("stop_ids", [])
            lines = watch.get("lines", [])
            direction = watch.get("direction", "")

            # Build the actual stop IDs with direction
            query_stop_ids = []
            for base in stop_ids_list:
                if direction:
                    query_stop_ids.append(base + direction)
                else:
                    query_stop_ids.append(base + "N")
                    query_stop_ids.append(base + "S")

            all_fetch_args.append({
                "lines": lines,
                "stop_ids": query_stop_ids,
                "watch": watch,
            })

        try:
            all_results = self._run_fetch_multi(
                all_fetch_args, max_arrivals
            )
        except Exception as e:
            logger.error(f"Failed to fetch arrivals: {e}")
            raise RuntimeError(
                f"Could not fetch departure data: {e}"
            )

        for fetch_arg, arrivals in zip(
            all_fetch_args, all_results
        ):
            watch = fetch_arg["watch"]
            north = watch.get("north_label", "Uptown")
            south = watch.get("south_label", "Downtown")

            deps = []
            for arrival in arrivals:
                arr_local = arrival.arrival_time.astimezone(tz)
                minutes = int(
                    (arr_local - now).total_seconds() / 60
                )
                if minutes < min_minutes:
                    continue
                dep_dir = (
                    north
                    if arrival.stop_id.endswith("N")
                    else south
                )
                # Map GTFS route_id back to display name
                # (e.g. "SI" -> "SIR", "7X" -> "7")
                display_route = GTFS_ROUTE_TO_LINE.get(
                    arrival.route_id, arrival.route_id
                )
                is_express = display_route in EXPRESS_TO_BASE
                display_route = EXPRESS_TO_BASE.get(
                    display_route, display_route
                )
                # Shuttles display as "S"
                display_label = SHUTTLE_DISPLAY.get(
                    display_route, display_route
                )
                deps.append({
                    "route_id": display_label,
                    "destination": (
                        arrival.destination or dep_dir
                    ),
                    "time": arr_local.strftime("%-I:%M %p"),
                    "minutes": minutes,
                    "color": LINE_COLORS.get(
                        display_route, "#808183"
                    ),
                    "direction": dep_dir,
                    "is_express": is_express,
                })

            deps.sort(key=lambda d: d["minutes"])

            # Build subtitle for this watch
            sub_parts = []
            line_filter = watch.get("line_filter", "")
            direction = watch.get("direction", "")
            if line_filter:
                sub_parts.append(f"{line_filter}")
            else:
                sub_parts.append("All Lines")
            if direction:
                sub_parts.append(
                    north if direction == "N" else south
                )

            station_groups.append({
                "name": watch.get("station_name", ""),
                "subtitle": " · ".join(sub_parts),
                "departures": deps,
            })

        # Distribute max_arrivals across groups
        per_group = max(
            1, max_arrivals // len(station_groups)
        )
        for group in station_groups:
            group["departures"] = (
                group["departures"][:per_group]
            )

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        template_params = {
            "station_groups": station_groups,
            "updated": now.strftime("%-I:%M %p"),
            "plugin_settings": settings,
        }

        return self.render_image(
            dimensions,
            "subway_departures.html",
            "subway_departures.css",
            template_params,
        )

    def _run_fetch_multi(self, fetch_args, max_per):
        """Fetch arrivals for multiple watches in one session."""
        async def _fetch():
            results = []
            async with _create_ssl_session() as session:
                for arg in fetch_args:
                    watch_arrivals = []
                    for route_id in arg["lines"]:
                        feed_id = LINE_TO_FEED.get(route_id)
                        if not feed_id:
                            continue
                        feed = SubwayFeed(
                            feed_id=feed_id,
                            session=session,
                        )
                        # Use the GTFS-RT route_id (e.g. "SI")
                        # instead of the display name (e.g. "SIR")
                        gtfs_route = LINE_TO_GTFS_ROUTE.get(
                            route_id, route_id
                        )
                        # Also query express variants (e.g. 7 → 7X)
                        gtfs_routes = [gtfs_route]
                        for expr, base in EXPRESS_TO_BASE.items():
                            if base == gtfs_route:
                                gtfs_routes.append(expr)
                        for gr in gtfs_routes:
                            for stop_id in arg["stop_ids"]:
                                try:
                                    arr = await feed.get_arrivals(
                                        route_id=gr,
                                        stop_id=stop_id,
                                        max_arrivals=max_per,
                                    )
                                    watch_arrivals.extend(arr)
                                except Exception:
                                    pass
                    results.append(watch_arrivals)
            return results
        return asyncio.run(_fetch())
