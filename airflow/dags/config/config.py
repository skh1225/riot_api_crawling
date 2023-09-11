schema = {
  'match': [{'name': 'matchId', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'tier', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'gameDuration', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'gameVersion', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'}, {'name': 'BLUE_WIN', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}, {'name': 'BLUE_TOP', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BLUE_JUG', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BLUE_MID', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BLUE_ADC', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BLUE_SUP', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_TOP', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_JUG', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_MID', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_ADC', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_SUP', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BANS', 'type': 'RECORD', 'fields':{'name':'list', 'type': 'RECORD','fields':{'name':'element', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 'mode': 'REPEATED'}, 'mode': 'NULLABLE'}, {'name': 'BLUE_BARON', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BLUE_DRAGON', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'BLUE_RIFT_HERALD', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_BARON', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_DRAGON', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'RED_RIFT_HERALD', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'execution_date', 'type': 'DATE', 'mode': 'NULLABLE'}],
  'pick': [{'name': 'tier', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'}, {'name': 'championId', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'win', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'pick', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'total', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'execution_date', 'type': 'DATE', 'mode': 'NULLABLE'}],
  'ban': [{'name': 'tier', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'}, {'name': 'championId', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'ban', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'total', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'execution_date', 'type': 'DATE', 'mode': 'NULLABLE'}],
  'champion': [{'name': 'tier', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},  {'name': 'gameVersion', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'gameDuration', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'matchId', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'championId', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'teamPosition', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'win', 'type': 'BOOL', 'mode': 'NULLABLE'}, {'name': 'damageTakenOnTeamPercentage', 'type': 'FLOAT64', 'mode': 'NULLABLE'}, {'name': 'teamDamagePercentage', 'type': 'FLOAT64', 'mode': 'NULLABLE'}, {'name': 'champExperience', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'goldEarned', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'totalMinionsKilled', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'magicDamageDealtToChampions', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'physicalDamageDealtToChampions', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'trueDamageDealtToChampions', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'damageDealtToBuildings', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'damageDealtToObjectives', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'totalDamageTaken', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'totalHeal', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'totalHealsOnTeammates', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'timeCCingOthers', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'kills', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'deaths', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'assists', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'wardsKilled', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'wardsPlaced', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'detectorWardsPlaced', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'visionScore', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'firstBloodKill', 'type': 'BOOL', 'mode': 'NULLABLE'}, {'name': 'firstTowerKill', 'type': 'BOOL', 'mode': 'NULLABLE'}, {'name': 'gameEndedInSurrender', 'type': 'BOOL', 'mode': 'NULLABLE'}, {'name': 'gameEndedInEarlySurrender', 'type': 'BOOL', 'mode': 'NULLABLE'}, {'name': 'execution_date', 'type': 'DATE', 'mode': 'NULLABLE'}]
}

resource = {
  "tableReference": {"projectId":"fourth-way-398009", "datasetId": "summoner_match","tableId": ""},
  "timePartitioning":{"type": "DAY", "expirationMs": "5184000000", "field": "date"},
  "requirePartitionFilter": True
  }

pyspark_job = {
    "placement": {
      "cluster_name": "spark-cluster"
    },
    "reference": {
      "project_id": "fourth-way-398009"
    },
    "pyspark_job": {
      "main_python_file_uri": "gs://summoner-match/pyspark/raw_to_processed.py",
      "properties": {
        "spark.executor.cores": "2",
        "spark.executor.memory": "6g",
        "spark.executor.instances": "3"
      },
      "jar_file_uris": [
        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
      ],
      "args": []
    }
  }

cluster_config = {
  "project_id": "fourth-way-398009",
  "cluster_name": "spark-cluster",
  "region": "asia-northeast3",
  "use_if_exists": True,
  "cluster_config": {
    "config_bucket": "summoner-match",
    "gce_cluster_config": {
      "network_uri": "default",
      "subnetwork_uri": "",
      "internal_ip_only": False,
      "zone_uri": "",
      "metadata": {},
      "tags": [],
      "shielded_instance_config": {
        "enable_secure_boot": False,
        "enable_vtpm": False,
        "enable_integrity_monitoring": False
      }
    },
    "master_config": {
      "num_instances": 1,
      "machine_type_uri": "n2-standard-2",
      "disk_config": {
        "boot_disk_type": "pd-standard",
        "boot_disk_size_gb": 100,
        "num_local_ssds": 0,
        "local_ssd_interface": "SCSI"
      },
      "min_cpu_platform": "",
      "image_uri": ""
    },
    "software_config": {
      "image_version": "2.1-debian11",
      "properties": {},
      "optional_components": [
        "JUPYTER"
      ]
    },
    "lifecycle_config": {
      "idle_delete_ttl": "1800s"
    },
    "initialization_actions": [],
    "encryption_config": {
      "gce_pd_kms_key_name": ""
    },
    "autoscaling_config": {
      "policy_uri": ""
    },
    "endpoint_config": {
      "enable_http_port_access": True
    },
    "security_config": {
      "kerberos_config": {}
    },
    "worker_config": {
      "num_instances": 3,
      "machine_type_uri": "n2-standard-2",
      "disk_config": {
        "boot_disk_type": "pd-standard",
        "boot_disk_size_gb": 100,
        "num_local_ssds": 0,
        "local_ssd_interface": "SCSI"
      },
      "min_cpu_platform": "",
      "image_uri": ""
    },
    "secondary_worker_config": {
      "num_instances": 0
    }
  }
}