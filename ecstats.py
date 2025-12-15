import os
import sys
import datetime
import configparser
import optparse
import boto3
import openpyxl

# Metric Collection Period (in days)
METRIC_COLLECTION_PERIOD_DAYS = int(
    os.environ.get("METRIC_COLLECTION_PERIOD_DAYS") or 7
)

SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE
SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR

RUNNING_INSTANCES_WORKSHEET_NAME = "ClusterData"
RESERVED_INSTANCES_WORKSHEET_NAME = "ReservedData"

# --- Excel/Formula injection mitigation ---
_DANGEROUS_EXCEL_PREFIXES = ("=", "+", "-", "@")


def sanitize_excel_value(v):
    """
    Sanitize values before writing to openpyxl.
    If a string starts with =, +, - or @, prefix with apostrophe to force text.
    """
    if isinstance(v, str) and v.startswith(_DANGEROUS_EXCEL_PREFIXES):
        return "'" + v
    return v


def sanitize_excel_row(values):
    """Sanitize a list/tuple of values (row) before writing to openpyxl."""
    if isinstance(values, (list, tuple)):
        return [sanitize_excel_value(x) for x in values]
    # fallback (shouldn't happen in this script, but safe)
    return sanitize_excel_value(values)


def get_max_metrics_hourly():
    metrics = [
        ("GetTypeCmds", "Maximum", SECONDS_IN_HOUR),
        ("SetTypeCmds", "Maximum", SECONDS_IN_HOUR),
        ("ClusterBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("EvalBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("GeoSpatialBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("HashBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("HyperLogLogBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("KeyBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("ListBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("PubSubBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("SetBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("SortedSetBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("StringBasedCmds", "Maximum", SECONDS_IN_HOUR),
        ("StreamBasedCmds", "Maximum", SECONDS_IN_HOUR),
    ]
    return metrics


def get_max_metrics_weekly():
    metrics = [
        ("CurrItems", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("BytesUsedForCache", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("CacheHits", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("CacheHitRate", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("CacheMisses", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("CurrConnections", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkBytesIn", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkBytesOut", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkPacketsIn", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkPacketsOut", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("EngineCPUUtilization", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("Evictions", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("ReplicationBytes", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("ReplicationLag", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("FreeableMemory", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("SwapUsage", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("DatabaseMemoryUsagePercentage", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkBandwidthInAllowanceExceeded", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkBandwidthOutAllowanceExceeded", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("NetworkPacketsPerSecondAllowanceExceeded", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("AuthenticationFailures", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("ChannelAuthorizationFailures", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("CommandAuthorizationFailures", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("KeyAuthorizationFailures", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("TrafficManagementActive", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("ClusterBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("EvalBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("GetTypeCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("KeyBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("ListBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("HashBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("PubSubBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("SetBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("SetTypeCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("SortedSetBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("StringBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
        ("StreamBasedCmdsLatency", "Maximum", SECONDS_IN_DAY * METRIC_COLLECTION_PERIOD_DAYS),
    ]
    return metrics


def calc_expiry_time(expiry):
    return (expiry.replace(tzinfo=None) - datetime.datetime.utcnow()).days


def get_clusters_info(session):
    conn = session.client("elasticache")
    results = {
        "elc_running_instances": {},
        "elc_reserved_instances": {},
    }

    paginator = conn.get_paginator("describe_cache_clusters")
    page_iterator = paginator.paginate(ShowCacheNodeInfo=True)

    snapshots = {}
    try:
        snapshots = conn.describe_snapshots()
    except:
        pass

    snaps = {}
    if "Snapshots" in snapshots:
        for snapshot in snapshots["Snapshots"]:
            try:
                if snapshot["SnapshotRetentionLimit"] > 0 and snapshot["ReplicationGroupId"]:
                    snaps[snapshot["ReplicationGroupId"]] = snapshot["SnapshotRetentionLimit"]
            except:
                pass

    for page in page_iterator:
        for instance in page["CacheClusters"]:
            if instance["CacheClusterStatus"] == "available" and (
                instance["Engine"] == "redis" or instance["Engine"] == "valkey"
            ):
                cluster_id = instance["CacheClusterId"]
                results["elc_running_instances"][cluster_id] = instance

    paginator = conn.get_paginator("describe_reserved_cache_nodes")
    page_iterator = paginator.paginate()

    for page in page_iterator:
        for reserved_instance in page["ReservedCacheNodes"]:
            if reserved_instance["State"] == "active" and (
                reserved_instance["ProductDescription"] == "redis"
                or reserved_instance["ProductDescription"] == "valkey"
            ):
                instance_type = reserved_instance["CacheNodeType"]
                expiry_time = reserved_instance["StartTime"] + datetime.timedelta(
                    seconds=reserved_instance["Duration"]
                )
                results["elc_reserved_instances"][instance_type] = {
                    "count": reserved_instance["CacheNodeCount"],
                    "expiry_time": calc_expiry_time(expiry=expiry_time),
                }

    results["snapshots"] = snaps
    return results


def get_metric(cloud_watch, cluster_id, node, metric, aggregation, period):
    today = datetime.date.today() + datetime.timedelta(days=1)
    then = today - datetime.timedelta(days=METRIC_COLLECTION_PERIOD_DAYS)
    response = cloud_watch.get_metric_statistics(
        Namespace="AWS/ElastiCache",
        MetricName=metric,
        Dimensions=[
            {"Name": "CacheClusterId", "Value": cluster_id},
            {"Name": "CacheNodeId", "Value": node},
        ],
        StartTime=then.isoformat(),
        EndTime=today.isoformat(),
        Period=period,
        Statistics=[aggregation],
    )

    raw_data = [rec[aggregation] for rec in response["Datapoints"]]
    return raw_data


def get_metric_curr(cloud_watch, cluster_id, node, metric):
    now = datetime.datetime.now()

    response = cloud_watch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "is_master_test",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/ElastiCache",
                        "MetricName": metric,
                        "Dimensions": [
                            {"Name": "CacheClusterId", "Value": cluster_id},
                            {"Name": "CacheNodeId", "Value": node},
                        ],
                    },
                    "Period": 60,
                    "Stat": "Maximum",
                    "Unit": "Count",
                },
                "Label": "string",
                "ReturnData": True,
            },
        ],
        StartTime=int(round(now.timestamp())) - SECONDS_IN_HOUR,
        EndTime=int(round(now.timestamp())),
        ScanBy="TimestampDescending",
        MaxDatapoints=1,
    )

    raw_data = [rec["Values"] for rec in response["MetricDataResults"]]
    try:
        return raw_data[0][0]
    except:
        return -1


def create_workbook(outDir, section, region_name):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = RUNNING_INSTANCES_WORKSHEET_NAME

    df_columns = [
        "Source",
        "ClusterId",
        "NodeId",
        "NodeRole",
        "NodeType",
        "Region",
        "SnapshotRetentionLimit",
    ]
    for metric, _, _ in get_max_metrics_weekly():
        df_columns.append(metric)
    for metric, _, _ in get_max_metrics_hourly():
        df_columns.append(metric)
    df_columns.append("Engine")
    df_columns.append("QPF")

    # sanitize header row too (safe, and keeps compliance consistent)
    ws.append(sanitize_excel_row(df_columns))

    ws = wb.create_sheet(RESERVED_INSTANCES_WORKSHEET_NAME)
    df_columns = ["Instance Type", "Count", "Remaining Time (days)"]
    ws.append(sanitize_excel_row(df_columns))
    return wb


def get_running_instances_metrics(wb, clusters_info, session):
    cloud_watch = session.client("cloudwatch")
    running_instances = clusters_info["elc_running_instances"]
    ws = wb[RUNNING_INSTANCES_WORKSHEET_NAME]
    row = []

    for instanceId, instanceDetails in running_instances.items():
        for node in instanceDetails.get("CacheNodes"):
            print("Fetching node %s details" % (instanceDetails["CacheClusterId"]))
            clusterId = instanceId
            if "ReplicationGroupId" in instanceDetails:
                clusterId = instanceDetails["ReplicationGroupId"]

            nodeRole = (
                "Master"
                if get_metric_curr(
                    cloud_watch, instanceId, node.get("CacheNodeId"), "IsMaster"
                )
                > 0
                else "Replica"
            )

            snapshotRetentionLimit = (
                clusters_info["snapshots"][clusterId]
                if clusterId in clusters_info["snapshots"]
                else -1
            )

            row.append("EC")
            row.append("%s" % clusterId)
            row.append("%s" % instanceId)
            row.append("%s" % nodeRole)
            row.append("%s" % instanceDetails["CacheNodeType"])
            row.append("%s" % instanceDetails["PreferredAvailabilityZone"])
            row.append("%s" % snapshotRetentionLimit)

            for metric, aggregation, period in get_max_metrics_weekly():
                data_points = get_metric(
                    cloud_watch,
                    instanceId,
                    node.get("CacheNodeId"),
                    metric,
                    aggregation,
                    period,
                )
                data_point = 0 if len(data_points) == 0 else data_points[0]
                row.append(data_point)

            for metric, aggregation, period in get_max_metrics_hourly():
                data_points = get_metric(
                    cloud_watch,
                    instanceId,
                    node.get("CacheNodeId"),
                    metric,
                    aggregation,
                    period,
                )
                data_point = 0 if len(data_points) == 0 else max(data_points)
                row.append(round(data_point / 60))

            row.append("%s" % instanceDetails["Engine"])
            row.append("")  # Empty qpf column

            # ✅ sanitize before writing
            ws.append(sanitize_excel_row(row))

            row = []
    return wb


def get_reserved_instances_info(wb, clusters_info):
    reserved_instances = clusters_info["elc_reserved_instances"]
    ws = wb[RESERVED_INSTANCES_WORKSHEET_NAME]
    for instanceId, instanceDetails in reserved_instances.items():
        row = [
            ("%s" % instanceId),
            ("%s" % instanceDetails["count"]),
            ("%s" % instanceDetails["expiry_time"]),
        ]
        ws.append(sanitize_excel_row(row))
    return wb


def process_aws_account(config, section, outDir):
    if config.has_option(section, "aws_access_key_id") and config.has_option(
        section, "aws_secret_access_key"
    ):
        aws_access_key_id = config.get(section, "aws_access_key_id")
        aws_secret_access_key = config.get(section, "aws_secret_access_key")
        region_name = config.get(section, "region_name")

        if config.has_option(section, "aws_session_token"):
            aws_session_token = config.get(section, "aws_session_token")
        else:
            aws_session_token = None

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )
    else:
        region_name = config.get(section, "region_name")
        session = boto3.Session(region_name=region_name)

    print(f"Requesting information for the {section} nodes")
    clusters_info = get_clusters_info(session)

    wb = create_workbook(outDir, section, region_name)
    wb = get_running_instances_metrics(wb, clusters_info, session)
    wb = get_reserved_instances_info(wb, clusters_info)

    output_file_path = "%s/%s-%s.xlsx" % (outDir, section, region_name)
    print(f"Writing output file {output_file_path}")
    wb.save(output_file_path)
    print("Done!")


def _demo_sanitization_print():
    samples = ["=1+1", "+SUM(1,2)", "-10+20", "@cmd", "normal", "  =not_triggered"]
    print("Original -> Sanitized")
    for s in samples:
        print(f"{s!r} -> {sanitize_excel_value(s)!r}")


def main():
    if not sys.version_info >= (3, 9):
        print("Please upgrade python to a version at least 3.9")
        exit(1)

    parser = optparse.OptionParser()
    parser.add_option(
        "-c",
        "--config",
        dest="configFile",
        default="config.ini",
        help="The filename for configuration file. By default the script will try to open the config.ini file.",
        metavar="FILE",
    )
    parser.add_option(
        "-d",
        "--out-dir",
        dest="outDir",
        default=".",
        help="The directory to output the results. If not the directory does not exist the script will try to create it.",
        metavar="PATH",
    )
    parser.add_option(
        "--demo-sanitize",
        action="store_true",
        dest="demo_sanitize",
        default=False,
        help="Print a sanitization demo and exit (no AWS calls).",
    )

    (options, _) = parser.parse_args()

    if options.demo_sanitize:
        _demo_sanitization_print()
        return

    if not os.path.isdir(options.outDir):
        os.makedirs(options.outDir)

    if not os.path.isfile(options.configFile):
        print(f"Can't find the specified {options.configFile} configuration file")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(options.configFile)

    for section in config.sections():
        process_aws_account(config, section, options.outDir)


if __name__ == "__main__":
    main()
